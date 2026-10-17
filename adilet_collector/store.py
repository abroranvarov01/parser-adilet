"""Состояние сбора в SQLite и атомарная запись файлов."""
from __future__ import annotations

import gzip
import json
import os
import re
import shutil
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path

SOURCE = "adilet"
LANG = "ru"  # язык по умолчанию (совместимость)
LANGS = ("ru", "kk")
SITE_SLUG = {"ru": "rus", "kk": "kaz"}   # путь на сайте: /rus/..., /kaz/...
SCHEMA_VERSION = 2

# Состояния документа
DISCOVERED = "discovered"
DOWNLOADING = "downloading"
DOWNLOADED = "downloaded"
PARSED = "parsed"
VALIDATED = "validated"      # полностью извлечён, опубликован в adilet_ru
INCOMPLETE = "incomplete"    # текст проверен, но есть непрочитанные приложения/изображения
REVIEW = "review"            # статус на странице не «действующий» или расходится — в quarantine
FAILED = "failed"            # ошибка загрузки (error_kind=download) или извлечения (extract)

SCHEMA = """
CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY, value TEXT);
CREATE TABLE IF NOT EXISTS catalog_pages(
  language TEXT NOT NULL DEFAULT 'ru', pass INTEGER NOT NULL, page INTEGER NOT NULL, status TEXT NOT NULL, url TEXT,
  n_cards INTEGER, n_new INTEGER, ids_hash TEXT, total_found INTEGER, total_pages INTEGER,
  raw_path TEXT, error TEXT, attempts INTEGER DEFAULT 0, next_try_at REAL DEFAULT 0, fetched_at TEXT,
  PRIMARY KEY(language, pass, page));
CREATE TABLE IF NOT EXISTS docs(
  source TEXT NOT NULL, doc_id TEXT NOT NULL, language TEXT NOT NULL,
  url TEXT NOT NULL, priority INTEGER DEFAULT 0,
  card_title TEXT, card_status_raw TEXT, card_status_class TEXT, card_requisites TEXT,
  discovered_from TEXT, first_seen_pass INTEGER, first_seen_page INTEGER, first_seen_at TEXT, last_seen_pass INTEGER,
  state TEXT NOT NULL DEFAULT 'discovered', attempts INTEGER DEFAULT 0, next_try_at REAL DEFAULT 0,
  error_kind TEXT, error TEXT, completeness TEXT,
  md_path TEXT, meta_path TEXT, raw_path TEXT, info_raw_path TEXT, quarantine_path TEXT,
  raw_sha256 TEXT, text_sha256 TEXT, page_status_raw TEXT, page_status_class TEXT, status_mismatch INTEGER,
  n_attachments INTEGER, fetched_at TEXT, updated_at TEXT, source_uuid TEXT, api_url TEXT, transport TEXT,
  PRIMARY KEY(source, doc_id, language));
CREATE INDEX IF NOT EXISTS docs_state ON docs(state, next_try_at);
CREATE TABLE IF NOT EXISTS events(ts TEXT, level TEXT, doc_id TEXT, kind TEXT, message TEXT, language TEXT);
CREATE TABLE IF NOT EXISTS saves(ts REAL, doc_id TEXT, state TEXT, language TEXT);
"""


def utcnow() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def atomic_write(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.tmp{os.getpid()}")
    with open(tmp, "wb") as f:
        f.write(data)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def atomic_write_gz(path: Path, data: bytes) -> None:
    atomic_write(path, gzip.compress(data, compresslevel=6, mtime=0))


def read_gz(path: Path) -> bytes:
    with gzip.open(path, "rb") as f:
        return f.read()


_UNSAFE = re.compile(r'[\\/:*?"<>|\x00-\x1f\x7f]')


def safe_title(title: str, max_chars: int = 80) -> str:
    t = _UNSAFE.sub("", title or "")
    t = re.sub(r"\s+", "_", t.strip())
    t = t[:max_chars]
    while len(t.encode("utf-8")) > 170:
        t = t[:-1]
    t = t.strip("._-")
    return t or "untitled"


class Paths:
    """Пути разделены по языку: версии ru и kk одного ID не перезаписывают друг друга."""

    def __init__(self, root: Path):
        self.root = root
        self.metadata = root / "metadata"
        self.raw = root / "raw"
        self.catalog_raw = root / "catalog_raw"
        self.attachments = root / "attachments"
        self.quarantine = root / "quarantine"
        self.incomplete = root / "incomplete"
        self.reports = root / "reports"
        self.backups = root / "backups"
        self.db = root / "state.sqlite"
        self.lock = root / ".collector.lock"

    def corpus(self, lang: str) -> Path:
        return self.root / f"adilet_{lang}"

    def ensure(self) -> None:
        for p in (self.metadata, self.raw, self.catalog_raw, self.attachments, self.quarantine,
                  self.incomplete, self.reports):
            p.mkdir(parents=True, exist_ok=True)
        for lang in LANGS:
            self.corpus(lang).mkdir(parents=True, exist_ok=True)

    def raw_doc(self, doc_id: str, lang: str) -> Path:
        return self.raw / lang / f"{doc_id}.html.gz"

    def raw_info(self, doc_id: str, lang: str) -> Path:
        return self.raw / lang / f"{doc_id}.info.html.gz"

    def md(self, doc_id: str, title: str, lang: str) -> Path:
        return self.corpus(lang) / f"{doc_id}_{safe_title(title)}.md"

    def md_incomplete(self, doc_id: str, title: str, lang: str) -> Path:
        return self.incomplete / lang / f"{doc_id}_{safe_title(title)}.md"

    def raw_api(self, doc_id: str, lang: str) -> Path:
        return self.raw / lang / f"{doc_id}.api.json.gz"

    def meta(self, doc_id: str, lang: str) -> Path:
        return self.metadata / lang / f"{doc_id}.json"

    def qdir(self, doc_id: str, lang: str) -> Path:
        return self.quarantine / lang / doc_id

    def catalog_page(self, lang: str, pass_no: int, page: int) -> Path:
        return self.catalog_raw / lang / f"pass{pass_no:02d}_page{page:05d}.html.gz"

    def rel(self, p: Path | None) -> str | None:
        if p is None:
            return None
        try:
            return str(Path(p).resolve().relative_to(self.root.resolve()))
        except ValueError:
            return str(p)


class Store:
    def __init__(self, paths: Paths):
        self.paths = paths
        paths.root.mkdir(parents=True, exist_ok=True)
        fresh = not paths.db.exists()
        self.db = sqlite3.connect(paths.db, isolation_level=None, timeout=30)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=NORMAL")
        if not fresh and self._version() < SCHEMA_VERSION:
            self.migrate_v2()
        self.db.executescript(SCHEMA)
        if fresh:
            self.set_meta("schema_version", SCHEMA_VERSION)
        paths.ensure()

    # -- миграция v1 -> v2 (добавление языка) -----------------------------------
    def _version(self) -> int:
        try:
            r = self.db.execute("SELECT value FROM meta WHERE key='schema_version'").fetchone()
        except sqlite3.OperationalError:
            return 1
        return int(json.loads(r[0])) if r else 1

    def backup(self, tag: str) -> Path:
        self.paths.backups.mkdir(parents=True, exist_ok=True)
        dst = self.paths.backups / f"state.sqlite.{tag}.{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        out = sqlite3.connect(dst)
        self.db.backup(out)
        out.close()
        return dst

    def migrate_v2(self) -> None:
        bak = self.backup("before_v2")
        print(f"миграция state.sqlite до схемы v2 (языки ru/kk); резервная копия: {bak}", flush=True)
        db = self.db
        cols = {r[1] for r in db.execute("PRAGMA table_info(catalog_pages)")}
        db.execute("BEGIN")
        try:
            if "language" not in cols:
                db.execute("ALTER TABLE catalog_pages RENAME TO catalog_pages_v1")
                db.execute("""CREATE TABLE catalog_pages(
                  language TEXT NOT NULL DEFAULT 'ru', pass INTEGER NOT NULL, page INTEGER NOT NULL, status TEXT NOT NULL,
                  url TEXT, n_cards INTEGER, n_new INTEGER, ids_hash TEXT, total_found INTEGER, total_pages INTEGER,
                  raw_path TEXT, error TEXT, attempts INTEGER DEFAULT 0, next_try_at REAL DEFAULT 0, fetched_at TEXT,
                  PRIMARY KEY(language, pass, page))""")
                db.execute("""INSERT INTO catalog_pages(language,pass,page,status,url,n_cards,n_new,ids_hash,total_found,
                  total_pages,raw_path,error,attempts,next_try_at,fetched_at)
                  SELECT 'ru',pass,page,status,url,n_cards,n_new,ids_hash,total_found,total_pages,raw_path,error,attempts,
                  next_try_at,fetched_at FROM catalog_pages_v1""")
                db.execute("DROP TABLE catalog_pages_v1")
            for table, col, typ in (("docs", "source_uuid", "TEXT"), ("docs", "api_url", "TEXT"), ("docs", "transport", "TEXT"),
                                    ("events", "language", "TEXT"), ("saves", "language", "TEXT")):
                if col not in {r[1] for r in db.execute(f"PRAGMA table_info({table})")}:
                    db.execute(f"ALTER TABLE {table} ADD COLUMN {col} {typ}")
            db.execute("UPDATE saves SET language='ru' WHERE language IS NULL")
            db.execute("UPDATE docs SET transport='old_html' WHERE transport IS NULL AND raw_path IS NOT NULL")
            # ключи meta каталога -> с языком
            for r in db.execute("SELECT key, value FROM meta").fetchall():
                k = r["key"]
                if k == "catalog_pass":
                    db.execute("INSERT OR REPLACE INTO meta VALUES('catalog_pass_ru', ?)", (r["value"],))
                elif k.startswith(("catalog_total_pages_", "catalog_total_found_", "catalog_complete_")) and k.rsplit("_", 1)[1].isdigit():
                    base, num = k.rsplit("_", 1)
                    db.execute("INSERT OR REPLACE INTO meta VALUES(?, ?)", (f"{base}_ru_{num}", r["value"]))
            # файлы ru -> подкаталоги ru
            moved = 0
            for d in ("raw", "metadata", "catalog_raw"):
                base = self.paths.root / d
                if not base.exists():
                    continue
                (base / "ru").mkdir(parents=True, exist_ok=True)
                for f in list(base.iterdir()):
                    if f.is_file() and not f.name.startswith("."):
                        os.replace(f, base / "ru" / f.name)
                        moved += 1
            q = self.paths.root / "quarantine"
            if q.exists():
                (q / "ru").mkdir(parents=True, exist_ok=True)
                for f in list(q.iterdir()):
                    if f.is_dir() and f.name not in LANGS:
                        os.replace(f, q / "ru" / f.name)
                        moved += 1
            for r in db.execute("SELECT doc_id, raw_path, meta_path, quarantine_path FROM docs WHERE language='ru'").fetchall():
                upd = {}
                for col, prefix in (("raw_path", "raw/"), ("meta_path", "metadata/"), ("quarantine_path", "quarantine/")):
                    v = r[col]
                    if v and v.startswith(prefix) and not v.startswith(prefix + "ru/"):
                        upd[col] = prefix + "ru/" + v[len(prefix):]
                if upd:
                    sets = ",".join(f"{k}=?" for k in upd)
                    db.execute(f"UPDATE docs SET {sets} WHERE source=? AND doc_id=? AND language='ru'",
                               list(upd.values()) + [SOURCE, r["doc_id"]])
            for r in db.execute("SELECT pass, page, raw_path FROM catalog_pages WHERE language='ru'").fetchall():
                v = r["raw_path"]
                if v and v.startswith("catalog_raw/") and not v.startswith("catalog_raw/ru/"):
                    db.execute("UPDATE catalog_pages SET raw_path=? WHERE language='ru' AND pass=? AND page=?",
                               ("catalog_raw/ru/" + v[len("catalog_raw/"):], r["pass"], r["page"]))
            db.execute("INSERT OR REPLACE INTO meta VALUES('schema_version', ?)", (json.dumps(SCHEMA_VERSION),))
            db.execute("INSERT INTO events VALUES(?,?,?,?,?,?)", (utcnow(), "info", None, "migration",
                                                                  f"v1->v2, файлов перемещено {moved}, backup {bak.name}", None))
            db.execute("COMMIT")
        except Exception:
            db.execute("ROLLBACK")
            raise
        print(f"миграция завершена: файлов перенесено в подкаталоги ru: {moved}", flush=True)

    # -- meta ------------------------------------------------------------------
    def get_meta(self, key: str, default=None):
        r = self.db.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        return json.loads(r[0]) if r else default

    def set_meta(self, key: str, value) -> None:
        self.db.execute("INSERT INTO meta(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                        (key, json.dumps(value, ensure_ascii=False)))

    def event(self, level: str, kind: str, message: str, doc_id: str | None = None, lang: str | None = None) -> None:
        self.db.execute("INSERT INTO events VALUES(?,?,?,?,?,?)", (utcnow(), level, doc_id, kind, message, lang))

    # -- recovery ----------------------------------------------------------------
    def recover(self) -> dict:
        """Незавершённые задачи — обратно в очередь (или на конвертацию, если оригинал уже сохранён)."""
        n_back = n_reparse = 0
        for r in self.db.execute("SELECT doc_id, language, raw_path FROM docs WHERE state=?", (DOWNLOADING,)).fetchall():
            have = self.paths.raw_doc(r["doc_id"], r["language"]).exists() or self.paths.raw_api(r["doc_id"], r["language"]).exists()
            new = DOWNLOADED if (have and r["raw_path"]) else DISCOVERED
            self.update_doc(r["doc_id"], r["language"], state=new)
            n_reparse += new == DOWNLOADED
            n_back += new == DISCOVERED
        self.db.execute("UPDATE catalog_pages SET status='pending' WHERE status='fetching'")
        return {"requeued": n_back, "to_reparse": n_reparse}

    # -- catalog -------------------------------------------------------------------
    def upsert_card(self, card: dict, lang: str, pass_no: int, page: int) -> bool:
        """True, если ID для этого языка новый."""
        cur = self.db.execute(
            """INSERT INTO docs(source,doc_id,language,url,card_title,card_status_raw,card_status_class,card_requisites,
                 discovered_from,first_seen_pass,first_seen_page,first_seen_at,last_seen_pass,state,updated_at)
               VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
               ON CONFLICT(source,doc_id,language) DO NOTHING""",
            (SOURCE, card["doc_id"], lang, card["url"], card["title"], card["status_raw"], card["status_class"],
             card["requisites"], "catalog", pass_no, page, utcnow(), pass_no, DISCOVERED, utcnow()))
        if cur.rowcount:
            return True
        self.db.execute(
            """UPDATE docs SET last_seen_pass=?, card_title=?, card_status_raw=?, card_status_class=?, card_requisites=?,
                 discovered_from=CASE WHEN discovered_from='manual' THEN 'manual+catalog' ELSE discovered_from END
               WHERE source=? AND doc_id=? AND language=?""",
            (pass_no, card["title"], card["status_raw"], card["status_class"], card["requisites"], SOURCE,
             card["doc_id"], lang))
        return False

    def add_manual(self, doc_id: str, lang: str, url: str) -> None:
        self.db.execute(
            """INSERT INTO docs(source,doc_id,language,url,priority,discovered_from,first_seen_at,state,updated_at)
               VALUES(?,?,?,?,1,'manual',?,?,?) ON CONFLICT(source,doc_id,language) DO UPDATE SET priority=1""",
            (SOURCE, doc_id, lang, url, utcnow(), DISCOVERED, utcnow()))

    def page_row(self, lang: str, pass_no: int, page: int):
        return self.db.execute("SELECT * FROM catalog_pages WHERE language=? AND pass=? AND page=?",
                               (lang, pass_no, page)).fetchone()

    def save_page(self, lang: str, pass_no: int, page: int, **kw) -> None:
        kw.setdefault("fetched_at", utcnow())
        cols = ["language", "pass", "page"] + list(kw)
        vals = [lang, pass_no, page] + list(kw.values())
        upd = ",".join(f"{k}=excluded.{k}" for k in kw)
        self.db.execute(f"INSERT INTO catalog_pages({','.join(cols)}) VALUES({','.join('?' * len(cols))}) "
                        f"ON CONFLICT(language,pass,page) DO UPDATE SET {upd}", vals)

    def page_hash(self, lang: str, pass_no: int, page: int) -> str | None:
        r = self.page_row(lang, pass_no, page)
        return r["ids_hash"] if r else None

    # -- docs ----------------------------------------------------------------------
    def doc(self, doc_id: str, lang: str):
        return self.db.execute("SELECT * FROM docs WHERE source=? AND doc_id=? AND language=?",
                               (SOURCE, doc_id, lang)).fetchone()

    def claim_next(self, langs: tuple, prefer: str | None = None):
        """Общая очередь для языков; prefer задаёт чередование ru/kk."""
        order = sorted(langs, key=lambda l: l != prefer)
        now = time.time()
        for lang in order:
            r = self.db.execute(
                "SELECT doc_id, language, url FROM docs WHERE state=? AND language=? AND next_try_at<=? "
                "ORDER BY priority DESC, rowid LIMIT 1", (DISCOVERED, lang, now)).fetchone()
            if r:
                self.update_doc(r["doc_id"], lang, state=DOWNLOADING)
                return r
        return None

    def update_doc(self, doc_id: str, lang: str, **kw) -> None:
        kw["updated_at"] = utcnow()
        sets = ",".join(f"{k}=?" for k in kw)
        self.db.execute(f"UPDATE docs SET {sets} WHERE source=? AND doc_id=? AND language=?",
                        list(kw.values()) + [SOURCE, doc_id, lang])

    def record_save(self, doc_id: str, lang: str, state: str) -> None:
        self.db.execute("INSERT INTO saves VALUES(?,?,?,?)", (time.time(), doc_id, state, lang))

    def saves_since(self, t: float, lang: str | None = None) -> int:
        if lang:
            return self.db.execute("SELECT COUNT(*) FROM saves WHERE ts>=? AND language=?", (t, lang)).fetchone()[0]
        return self.db.execute("SELECT COUNT(*) FROM saves WHERE ts>=?", (t,)).fetchone()[0]

    def counts(self, lang: str | None = None) -> dict:
        w, a = ("WHERE language=?", (lang,)) if lang else ("", ())
        c = {r[0]: r[1] for r in self.db.execute(f"SELECT state, COUNT(*) FROM docs {w} GROUP BY state", a)}
        fk = {r[0]: r[1] for r in self.db.execute(
            f"SELECT error_kind, COUNT(*) FROM docs WHERE state=? {'AND language=?' if lang else ''} GROUP BY error_kind",
            (FAILED,) + a)}
        c["_failed_download"] = fk.get("download", 0)
        c["_failed_extract"] = fk.get("extract", 0)
        c["_total"] = sum(v for k, v in c.items() if not k.startswith("_"))
        c["_pending_retry"] = self.db.execute(
            f"SELECT COUNT(*) FROM docs WHERE state=? AND attempts>0 {'AND language=?' if lang else ''}",
            (DISCOVERED,) + a).fetchone()[0]
        return c

    def remove_stale_md(self, doc_id: str, lang: str, keep: Path | None) -> None:
        """Одна актуальная копия документа на языке: удаляем прежние файлы этого ID (в корпусе и в incomplete)."""
        for d in (self.paths.corpus(lang), self.paths.incomplete / lang):
            if not d.exists():
                continue
            for p in d.glob(f"{doc_id}_*.md"):
                if p.name[: len(doc_id) + 1] == f"{doc_id}_" and (keep is None or p.resolve() != keep.resolve()):
                    p.unlink(missing_ok=True)

    def clear_quarantine(self, doc_id: str, lang: str) -> None:
        q = self.paths.qdir(doc_id, lang)
        if q.exists():
            shutil.rmtree(q)
