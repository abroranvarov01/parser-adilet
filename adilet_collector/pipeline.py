"""Конвейер: каталоги ru/kk -> общая очередь (SQLite) -> скачивание -> извлечение и проверка -> сохранение."""
from __future__ import annotations

import asyncio
import json
import os
import shutil
import signal
import time
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from urllib.parse import urlsplit

from . import PARSER_VERSION
from .catalog import CatalogError, page_url, parse_page
from .document import (API_LANG, BASE_URL, STATUS_VERIFIED, PageError, api_url, dotted_to_iso, parse_api_document,
                       parse_document, parse_info, sha256)
from .net import Blocked, Controller, FetchError, Http, RobotsDisallowed
from .store import (DISCOVERED, DOWNLOADED, DOWNLOADING, FAILED, INCOMPLETE, LANGS, PARSED, REVIEW, SITE_SLUG,
                    VALIDATED, Paths, Store, atomic_write, atomic_write_gz, read_gz, utcnow)

ACTIVE_CLASSES = {"status_new", "status_upd"}


@dataclass
class Config:
    data_dir: Path
    langs: tuple = LANGS
    rps: float = 2.0
    start_concurrency: int = 2
    max_concurrency: int = 8
    transport: str = "aiohttp"
    user_agent: str = "Mozilla/5.0 (compatible; TrafficLawAdiletCollector/1.0)"
    timeout: float = 180.0
    max_retries: int = 5
    backoff_base: float = 3.0
    backoff_cap: float = 120.0
    adapt_window: float = 120.0
    pagesize: int = 100
    max_pages: int | None = None
    max_docs: int | None = None
    catalog: bool = True
    ids: tuple = ()          # (lang, doc_id)
    with_info: bool = False
    fetch_attachments: bool = False
    attachments_ignore_robots: bool = False
    publish_incomplete: bool = False
    doc_transport: str = "old_html"   # old_html | api (api — только по явному решению оператора)
    api_fallback: bool = True         # при ошибке/заглушке API — разрешённая страница старого сайта
    doc_attempts: int = 3
    verbose: bool = False
    progress_every: float = 15.0


def parse_ids(spec: str | None, langs: tuple) -> list[tuple[str, str]]:
    """«K1400000235,kk:Z1400000194» -> [(ru,K14..),(kk,K14..),(kk,Z14..)]; без префикса — для всех языков."""
    out = []
    for item in (spec or "").split(","):
        item = item.strip()
        if not item:
            continue
        if ":" in item:
            lang, i = item.split(":", 1)
            out.append((lang.strip(), i.strip()))
        else:
            out += [(l, item) for l in langs]
    return out


# ---------------------------------------------------------------------------
# Обработка одного документа (выполняется в отдельном процессе)
# ---------------------------------------------------------------------------
def _parse_job(raw: bytes, info_raw: bytes | None, doc_id: str, url: str) -> dict:
    html_text = raw.decode("utf-8", errors="replace")
    try:
        res = parse_document(html_text, doc_id, url)
    except PageError as e:
        return {"error": e.kind, "detail": e.detail}
    if "�" in html_text:
        res["warnings"].append("utf8_replacement_chars_in_source")
    res["headings"] = dict(res["headings"])
    if info_raw is not None:
        try:
            res["info"] = parse_info(info_raw.decode("utf-8", errors="replace"))
        except PageError as e:
            res["warnings"].append(f"info:{e.kind}")
    return res


def _parse_api_job(raw: bytes, doc_id: str, lang: str, url: str) -> dict:
    try:
        res = parse_api_document(raw, doc_id, lang, url)
    except PageError as e:
        return {"error": e.kind, "detail": e.detail}
    res["headings"] = dict(res["headings"])
    return res


LABELS = {
    "ru": ("Реквизиты", "Статус на сайте", "Идентификатор", "Источник"),
    "kk": ("Деректемелері", "Сайттағы мәртебесі", "Сәйкестендіргіш", "Дереккөз"),
}


def build_markdown(title: str, doc_id: str, url: str, res: dict, lang: str) -> str:
    req_l, st_l, id_l, src_l = LABELS[lang]
    lines = [f"# {title}", ""]
    if res.get("requisites"):
        lines.append(f"{req_l}: {res['requisites']}  ")
    st = res["page_status"]["raw"]
    if st:
        lines.append(f"{st_l}: {st}  ")
    lines.append(f"{id_l}: {doc_id}  ")
    lines.append(f"{src_l}: {url}")
    lines.append("")
    return "\n".join(lines) + "\n" + res["body_md"]


class Collector:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.paths = Paths(cfg.data_dir)
        self.store = Store(self.paths)
        self.blocked: str | None = None
        self.docs_started = 0
        self.catalog_done = False
        self.consecutive_fail = 0
        self.t0 = time.time()
        self.pool: ProcessPoolExecutor | None = None
        self._turn = 0
        self._att_results: dict = {}

    def log(self, msg: str, verbose: bool = False) -> None:
        if verbose and not self.cfg.verbose:
            return
        print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)

    # -- сохранение результата -------------------------------------------------
    def finalize(self, doc_id: str, lang: str, res: dict, raw: bytes, info_raw: bytes | None,
                 fetched_at: str | None) -> str:
        row = self.store.doc(doc_id, lang)
        url = row["url"]
        transport = row["transport"] or "old_html"
        raw_path = self.paths.raw_api(doc_id, lang) if transport == "api" else self.paths.raw_doc(doc_id, lang)
        if "error" in res:
            return self._fail_extract(doc_id, lang, res["error"], res.get("detail", ""), raw_path)

        title = res["title"]
        page_st = res["page_status"]
        card_cls = row["card_status_class"]
        status_mismatch = bool(card_cls) and card_cls != page_st["css_class"]
        val = res["validation"]
        warnings = list(res["warnings"])
        if row["card_title"] and row["card_title"] != title:
            warnings.append("card_title_differs_from_page_title")
        if status_mismatch:
            warnings.append(f"status_mismatch:card={card_cls}/page={page_st['css_class']}")

        attachments = self._attachments_meta(doc_id, lang, res["attachments"])
        content_att = [a for a in attachments if not a.get("decorative")]
        if not val["ok"]:
            completeness = "extraction_failed"
        elif content_att:
            completeness = "attachments_not_extracted"
        else:
            completeness = "complete"

        reasons = []
        if not val["ok"]:
            reasons += val["problems"]
        if page_st["css_class"] not in ACTIVE_CLASSES:
            reasons.append(f"page_status_not_active:{page_st['raw']}")
        if status_mismatch:
            reasons.append("status_mismatch")
        lang_mismatch = [w for w in warnings if w.startswith("text_language_mismatch")]
        reasons += lang_mismatch

        if not val["ok"]:
            state, error_kind = FAILED, "extract"
        elif page_st["css_class"] not in ACTIVE_CLASSES or status_mismatch or lang_mismatch:
            state, error_kind = REVIEW, None
        elif completeness == "attachments_not_extracted":
            state, error_kind = INCOMPLETE, None
            reasons.append(f"content_attachments_not_extracted:{len(content_att)}")
        else:
            state, error_kind = VALIDATED, None

        md_text = build_markdown(title, doc_id, url, res, lang)
        info = res.get("info") or {}
        qdir = md_path = None
        if state in (FAILED, REVIEW):
            qdir = self.paths.qdir(doc_id, lang)
        elif state == INCOMPLETE and not self.cfg.publish_incomplete:
            md_path = self.paths.md_incomplete(doc_id, title, lang)   # отдельно от готового корпуса
        else:
            md_path = self.paths.md(doc_id, title, lang)

        info_adopt = dotted_to_iso(info.get("Дата принятия акта"))
        api = res.get("api")
        if api:
            info_adopt = None  # даты берутся из JSON API, вкладка «Информация» не запрашивается
        meta = {
            "source_system": "adilet",
            "jurisdiction": "KZ",
            "document_id": doc_id,
            "source_uuid": row["source_uuid"],
            "language": lang,
            "site_language": SITE_SLUG[lang],
            "title": title,
            "card_title": row["card_title"],
            "requisites_raw": res.get("requisites"),
            "source_url": url,
            "api_url": row["api_url"] if transport == "api" else None,
            "transport": transport,
            "robots_note": ("adilet.zan.kz/robots.txt запрещает /api/; API использован по решению оператора "
                            "(--i-accept-api-robots-risk)") if transport == "api" else None,
            "api": api,
            "status_raw": page_st["raw"],
            "status_css_class": page_st["css_class"],
            "normalized_status": page_st["normalized"],
            "card_status_raw": row["card_status_raw"],
            "card_status_css_class": card_cls,
            "card_normalized_status": STATUS_VERIFIED.get((card_cls, row["card_status_raw"])),
            "status_mismatch": status_mismatch,
            "status_mapping_note": "status_new/status_upd = выдача фильтра st=new|upd (ru «Действующий», kk «Қолданылып жүрген»); "
                                   "«Новый/Жаңа» — статус сайта, не дата принятия. Статус каждого языка свой.",
            "catalog_filter": "st=new|upd" if "catalog" in (row["discovered_from"] or "") else None,
            "discovered_from": row["discovered_from"],
            "dates": {
                "adoption_date": (api["dates_calendar"]["adoption_date"] if api else None) or info_adopt
                                 or res.get("adoption_date_from_requisites"),
                "adoption_date_source": ("api" if api and api["dates_calendar"]["adoption_date"] else
                                         "info_tab" if info_adopt else
                                         "requisites_line" if res.get("adoption_date_from_requisites") else None),
                "modification_date": None if api else dotted_to_iso(info.get("Дата изменения акта")),
                "adilet_publication_date": (api["dates_calendar"]["publication_date"] if api else
                                            dotted_to_iso(info.get('Дата официальной публикации в ИПС "Әділет"'))),
                "api_dates_raw": api["dates_raw"] if api else None,
                "entry_into_force_date": None,
                "entry_into_force_raw": res.get("entry_into_force_raw") or [],
            },
            "info_tab": info or None,
            "fetched_at": fetched_at,
            "processed_at": utcnow(),
            "paths": {
                "markdown": self.paths.rel(md_path) if md_path else None,
                "quarantine": self.paths.rel(qdir) if qdir else None,
                "raw": self.paths.rel(raw_path),
                "raw_info": self.paths.rel(self.paths.raw_info(doc_id, lang)) if info_raw is not None else None,
            },
            "raw_sha256": sha256(raw),
            "text_sha256": sha256(res["body_md"]),
            "parser_version": PARSER_VERSION,
            "attachments": attachments,
            "structure": {"headings_by_level": res["headings"], "tables": res["tables"],
                          "html_tables": res["html_tables"], "links": res["internal_links"]},
            "completeness": completeness,
            "state": state,
            "validation": val,
            "review_reasons": reasons,
            "warnings": warnings,
        }
        meta_json = json.dumps(meta, ensure_ascii=False, indent=2).encode("utf-8")
        if qdir is not None:
            qdir.mkdir(parents=True, exist_ok=True)
            atomic_write(qdir / "document.md", md_text.encode("utf-8"))
            atomic_write(qdir / "metadata.json", meta_json)
            self._link_raw(raw_path, qdir / raw_path.name)
            self.store.remove_stale_md(doc_id, lang, None)
        else:
            atomic_write(md_path, md_text.encode("utf-8"))
            self.store.remove_stale_md(doc_id, lang, md_path)
            self.store.clear_quarantine(doc_id, lang)
        atomic_write(self.paths.meta(doc_id, lang), meta_json)
        self.store.update_doc(
            doc_id, lang, state=state, error_kind=error_kind, error="; ".join(reasons) or None,
            completeness=completeness, md_path=self.paths.rel(md_path) if md_path else None,
            meta_path=self.paths.rel(self.paths.meta(doc_id, lang)), quarantine_path=self.paths.rel(qdir) if qdir else None,
            raw_sha256=meta["raw_sha256"], text_sha256=meta["text_sha256"], page_status_raw=page_st["raw"],
            page_status_class=page_st["css_class"], status_mismatch=int(status_mismatch),
            n_attachments=len(content_att), source_uuid=(api or {}).get("id") or row["source_uuid"])
        self.store.record_save(doc_id, lang, state)
        return state

    def _attachments_meta(self, doc_id: str, lang: str, items: list[dict]) -> list[dict]:
        out = []
        for a in items:
            host = urlsplit(a["url"]).netloc
            same = host in ("old.adilet.zan.kz", "adilet.zan.kz")
            prev = self._att_results.get((lang, doc_id, a["url"]))
            out.append({
                **a,
                "same_site": same,
                "robots_allowed": self.http_allowed(a["url"]) if same else None,
                "download_status": prev["download_status"] if prev else "not_downloaded",
                "local_path": prev.get("local_path") if prev else None,
                "sha256": prev.get("sha256") if prev else None,
                "content_type": prev.get("content_type") if prev else None,
                "content_extracted": False,
            })
        return out

    def http_allowed(self, url: str) -> bool | None:
        http = getattr(self, "http", None)
        if http is not None:
            return http.allowed(url)
        if not hasattr(self, "_robots"):  # офлайн-конвертация: правила из сохранённого robots.txt
            from urllib import robotparser
            txt = self.store.get_meta("robots_txt")
            self._robots = None
            if txt:
                self._robots = robotparser.RobotFileParser()
                self._robots.parse(txt.splitlines())
        return self._robots.can_fetch(self.cfg.user_agent, url) if self._robots else None

    def _link_raw(self, src: Path, dst: Path) -> None:
        if dst.exists():
            dst.unlink()
        try:
            os.link(src, dst)
        except OSError:
            shutil.copy2(src, dst)

    def _fail_extract(self, doc_id: str, lang: str, kind: str, detail: str, raw_path: Path) -> str:
        qdir = self.paths.qdir(doc_id, lang)
        qdir.mkdir(parents=True, exist_ok=True)
        if raw_path.exists():
            self._link_raw(raw_path, qdir / raw_path.name)
        atomic_write(qdir / "error.json", json.dumps({"document_id": doc_id, "language": lang, "error_kind": kind,
                                                      "detail": detail, "at": utcnow(),
                                                      "parser_version": PARSER_VERSION},
                                                     ensure_ascii=False, indent=2).encode())
        self.store.remove_stale_md(doc_id, lang, None)
        self.store.update_doc(doc_id, lang, state=FAILED, error_kind="extract", error=f"{kind}: {detail}",
                              completeness="extraction_failed", md_path=None, quarantine_path=self.paths.rel(qdir))
        self.store.record_save(doc_id, lang, FAILED)
        return FAILED

    # -- конвертация из сохранённого оригинала -----------------------------------
    async def parse_and_save(self, doc_id: str, lang: str, fetched_at: str | None = None) -> str:
        row = self.store.doc(doc_id, lang)
        loop = asyncio.get_running_loop()
        if row["transport"] == "api" and self.paths.raw_api(doc_id, lang).exists():
            raw = read_gz(self.paths.raw_api(doc_id, lang))
            info_raw = None
            res = await loop.run_in_executor(self.pool, _parse_api_job, raw, doc_id, lang, row["url"])
        else:
            raw = read_gz(self.paths.raw_doc(doc_id, lang))
            info_p = self.paths.raw_info(doc_id, lang)
            info_raw = read_gz(info_p) if info_p.exists() else None
            res = await loop.run_in_executor(self.pool, _parse_job, raw, info_raw, doc_id, row["url"])
        if "error" not in res:
            self.store.update_doc(doc_id, lang, state=PARSED)
        return self.finalize(doc_id, lang, res, raw, info_raw, fetched_at or row["fetched_at"])

    # -- сеть: документ ---------------------------------------------------------------
    async def process_doc_api(self, doc_id: str, lang: str, url: str) -> bool:
        """True — документ сохранён через API; False — нужен резервный путь (страница старого сайта)."""
        row = self.store.doc(doc_id, lang)
        au = api_url(doc_id, lang)
        try:
            r = await self.http.get(au, kind="api", check_robots=False, headers={"Accept": "application/json"})
        except FetchError as e:
            self.store.event("warn", "api_fetch_failed", str(e), doc_id, lang)
            if e.status == 404 and not self.cfg.api_fallback:
                self._download_failed(doc_id, lang, row, f"API: {e}", permanent=True)
                return True
            if not self.cfg.api_fallback:
                self._download_failed(doc_id, lang, row, f"API: {e}")
                return True
            return False
        loop = asyncio.get_running_loop()
        res = await loop.run_in_executor(self.pool, _parse_api_job, r.body, doc_id, lang, url)
        if "error" in res:
            if res["error"] == "blocked":
                raise Blocked(f"API {lang}:{doc_id}: {res['detail']}")
            self.store.event("warn", "api_invalid", f"{res['error']}: {res['detail']}", doc_id, lang)
            self.log(f"  {lang}:{doc_id}: ответ API отклонён ({res['error']}: {res['detail']})", verbose=True)
            if not self.cfg.api_fallback:
                self._download_failed(doc_id, lang, row, f"API {res['error']}: {res['detail']}", permanent=True)
                return True
            return False
        fetched_at = utcnow()
        atomic_write_gz(self.paths.raw_api(doc_id, lang), r.body)
        self.store.update_doc(doc_id, lang, state=DOWNLOADED, raw_path=self.paths.rel(self.paths.raw_api(doc_id, lang)),
                              fetched_at=fetched_at, attempts=(row["attempts"] or 0) + 1, error=None, error_kind=None,
                              transport="api", api_url=au, source_uuid=res["api"]["id"])
        self.consecutive_fail = 0
        state = self.finalize(doc_id, lang, res, r.body, None, fetched_at)
        self.log(f"  {lang}:{doc_id}: {state} (api)", verbose=True)
        return True

    async def process_doc(self, doc_id: str, lang: str, url: str) -> None:
        if self.cfg.doc_transport == "api":
            if await self.process_doc_api(doc_id, lang, url):
                return
            self.log(f"  {lang}:{doc_id}: резервная загрузка со страницы старого сайта", verbose=True)
        row = self.store.doc(doc_id, lang)
        try:
            r = await self.http.get(url, kind="doc")
            final_path = urlsplit(r.url).path.rstrip("/")
            if final_path != f"/{SITE_SLUG[lang]}/docs/{doc_id}":
                raise PageError("redirected", f"перенаправлено на {r.url}")
            html_text = r.text()
            from bs4 import BeautifulSoup
            from .document import check_document_page
            check_document_page(BeautifulSoup(html_text, "lxml"), html_text, doc_id)
            fetched_at = utcnow()
            atomic_write_gz(self.paths.raw_doc(doc_id, lang), r.body)
            self.store.update_doc(doc_id, lang, state=DOWNLOADED, raw_path=self.paths.rel(self.paths.raw_doc(doc_id, lang)),
                                  fetched_at=fetched_at, attempts=(row["attempts"] or 0) + 1, error=None,
                                  error_kind=None, transport="old_html")
            if self.cfg.with_info:
                try:
                    ri = await self.http.get(url + "/info", kind="info")
                    atomic_write_gz(self.paths.raw_info(doc_id, lang), ri.body)
                except (FetchError, RobotsDisallowed) as e:
                    self.store.event("warn", "info_fetch_failed", str(e), doc_id, lang)
            if self.cfg.fetch_attachments:
                await self.fetch_attachments(doc_id, lang, html_text, url)
            self.consecutive_fail = 0
        except PageError as e:
            if e.kind == "blocked":
                raise Blocked(f"{lang}:{doc_id}: {e.detail}")
            self._download_failed(doc_id, lang, row, f"{e.kind}: {e.detail}")
            return
        except FetchError as e:
            self._download_failed(doc_id, lang, row, str(e), permanent=e.status == 404)
            return
        except RobotsDisallowed as e:
            self.store.update_doc(doc_id, lang, state=FAILED, error_kind="download", error=f"robots.txt запрещает {e}")
            return
        state = await self.parse_and_save(doc_id, lang, fetched_at)
        self.log(f"  {lang}:{doc_id}: {state}", verbose=True)

    def _download_failed(self, doc_id: str, lang: str, row, err: str, permanent: bool = False) -> None:
        self.consecutive_fail += 1
        attempts = (row["attempts"] or 0) + 1
        if permanent or attempts >= self.cfg.doc_attempts:
            self.store.update_doc(doc_id, lang, state=FAILED, error_kind="download", error=err, attempts=attempts)
            self.store.record_save(doc_id, lang, FAILED)
            self.log(f"  {lang}:{doc_id}: ошибка загрузки ({err})")
        else:
            self.store.update_doc(doc_id, lang, state=DISCOVERED, error=err, attempts=attempts,
                                  next_try_at=time.time() + 300 * attempts)
            self.log(f"  {lang}:{doc_id}: отложен для повтора ({err})", verbose=True)
        if self.consecutive_fail >= 15:
            raise Blocked(f"{self.consecutive_fail} неудачных документов подряд — возможна блокировка или недоступность сайта")

    async def fetch_attachments(self, doc_id: str, lang: str, html_text: str, url: str) -> None:
        import re as _re
        from bs4 import BeautifulSoup
        from .document import ATTACH_EXT, Converter, preprocess_container
        soup = BeautifulSoup(html_text, "lxml")
        art = soup.select_one(".container_gamma.text article")
        if art is None:
            return
        conv = Converter(url)
        conv.blocks(preprocess_container(art)[0])

        def is_file(u):
            path = urlsplit(u).path
            return "/files/" in path or ATTACH_EXT.search(path) or _re.search(r"/download(?:/|$)", path)

        urls = [m["url"] for m in conv.media] + [l["url"] for l in conv.links if is_file(l["url"])]
        for u in dict.fromkeys(urls):
            host = urlsplit(u).netloc
            rec = {"download_status": "skipped_external"}
            if host in ("old.adilet.zan.kz", "adilet.zan.kz"):
                if not self.http.allowed(u) and not self.cfg.attachments_ignore_robots:
                    rec = {"download_status": "skipped_robots_disallow"}
                else:
                    try:
                        r = await self.http.get(u, kind="attachment", check_robots=not self.cfg.attachments_ignore_robots)
                        name = os.path.basename(urlsplit(u).path) or "file"
                        dst = self.paths.attachments / lang / doc_id / f"{sha256(u)[:10]}_{name}"
                        atomic_write(dst, r.body)
                        rec = {"download_status": "downloaded", "local_path": self.paths.rel(dst),
                               "sha256": sha256(r.body), "content_type": r.headers.get("content-type")}
                    except (FetchError, RobotsDisallowed) as e:
                        rec = {"download_status": f"failed: {e}"}
            self._att_results[(lang, doc_id, u)] = rec

    # -- сеть: каталоги ------------------------------------------------------------------
    def pass_no(self, lang: str) -> int:
        return self.store.get_meta(f"catalog_pass_{lang}", 1)

    async def run_catalog(self) -> None:
        """Каталоги языков обходятся по очереди (ru-стр., kk-стр., ...) через общий лимит."""
        try:
            pages = {l: 1 for l in self.cfg.langs}
            active = list(self.cfg.langs)
            while active and not self.stopping.is_set():
                for lang in list(active):
                    if self.stopping.is_set():
                        break
                    pass_no = self.pass_no(lang)
                    total = self.store.get_meta(f"catalog_total_pages_{lang}_{pass_no}")
                    page = pages[lang]
                    while True:  # пропуск готовых страниц
                        if (self.cfg.max_pages and page > self.cfg.max_pages) or (total and page > total):
                            page = None
                            break
                        row = self.store.page_row(lang, pass_no, page)
                        if row and (row["status"] == "done" or (row["status"] == "failed" and row["attempts"] >= 6)):
                            page += 1
                            continue
                        break
                    if page is None:
                        active.remove(lang)
                        self._catalog_summary(lang, pass_no)
                        continue
                    ok = await self.fetch_catalog_page(lang, pass_no, page)
                    if ok is None:
                        await asyncio.sleep(10)
                        pages[lang] = page
                    else:
                        pages[lang] = page + 1
        finally:
            self.catalog_done = True

    def _catalog_summary(self, lang: str, pass_no: int) -> None:
        total = self.store.get_meta(f"catalog_total_pages_{lang}_{pass_no}")
        if not total or self.cfg.max_pages:
            return
        done = self.store.db.execute("SELECT COUNT(*) FROM catalog_pages WHERE language=? AND pass=? AND status='done'",
                                     (lang, pass_no)).fetchone()[0]
        if done >= total:
            self.store.set_meta(f"catalog_complete_{lang}_{pass_no}", utcnow())
            self.log(f"каталог {lang} (проход {pass_no}) пройден полностью: {done} страниц")
        else:
            self.log(f"каталог {lang}: {total - done} страниц с ошибками — повтор: retry-failed --kind catalog")

    async def fetch_catalog_page(self, lang: str, pass_no: int, page: int) -> bool | None:
        url = page_url(page, self.cfg.pagesize, lang=lang)
        row = self.store.page_row(lang, pass_no, page)
        attempts = (row["attempts"] if row else 0) + 1
        try:
            r = await self.http.get(url, kind="catalog")
            res = parse_page(r.text(), url, page, self.cfg.pagesize, lang=lang)
            prev_hash = self.store.page_hash(lang, pass_no, page - 1)
            if prev_hash and prev_hash == res["ids_hash"]:
                raise CatalogError("same_as_previous", f"страница {page} совпадает с {page - 1}")
        except (CatalogError, FetchError) as e:
            if getattr(e, "kind", None) == "blocked":
                raise Blocked(f"каталог {lang} стр. {page}: {e}")
            self.store.save_page(lang, pass_no, page, status="failed" if attempts >= 6 else "pending", url=url,
                                 error=str(e), attempts=attempts)
            self.log(f"каталог {lang} стр. {page}: {e} (попытка {attempts})")
            return None if attempts < 6 else False
        raw_p = self.paths.catalog_page(lang, pass_no, page)
        atomic_write_gz(raw_p, r.body)
        n_new = sum(self.store.upsert_card(c, lang, pass_no, page) for c in res["cards"])
        key = f"catalog_total_found_{lang}_{pass_no}"
        old_total = self.store.get_meta(key)
        if old_total is not None and res["total_found"] is not None and old_total != res["total_found"]:
            self.store.event("warn", "catalog_changed", f"стр. {page}: всего было {old_total}, стало {res['total_found']}",
                             lang=lang)
            self.log(f"каталог {lang} изменился во время обхода: {old_total} -> {res['total_found']} документов")
        self.store.set_meta(key, res["total_found"])
        if res["total_pages"]:
            self.store.set_meta(f"catalog_total_pages_{lang}_{pass_no}", res["total_pages"])
        for w in res["warnings"]:
            self.store.event("warn", "catalog_page", f"стр. {page}: {w}", lang=lang)
        self.store.save_page(lang, pass_no, page, status="done", url=url, n_cards=len(res["cards"]), n_new=n_new,
                             ids_hash=res["ids_hash"], total_found=res["total_found"], total_pages=res["total_pages"],
                             raw_path=self.paths.rel(raw_p), error=None, attempts=attempts)
        self.log(f"каталог {lang} стр. {page}/{res['total_pages']}: карточек {len(res['cards'])}, новых ID {n_new}"
                 + (f", предупреждения: {res['warnings']}" if res["warnings"] else ""))
        return True

    # -- воркеры ------------------------------------------------------------------------------
    async def worker(self) -> None:
        while not self.stopping.is_set():
            if self.cfg.max_docs is not None and self.docs_started >= self.cfg.max_docs:
                return
            prefer = self.cfg.langs[self._turn % len(self.cfg.langs)]
            self._turn += 1
            job = self.store.claim_next(self.cfg.langs, prefer)
            if job is None:
                q = ",".join("?" * len(self.cfg.langs))
                waiting = self.store.db.execute(
                    f"SELECT COUNT(*) FROM docs WHERE state=? AND language IN ({q})", (DISCOVERED, *self.cfg.langs)).fetchone()[0]
                if self.catalog_done and waiting == 0:
                    return
                await asyncio.sleep(2)
                continue
            self.docs_started += 1
            doc_id, lang = job["doc_id"], job["language"]
            try:
                await self.process_doc(doc_id, lang, job["url"])
            except Blocked as e:
                self.blocked = str(e)
                self.store.update_doc(doc_id, lang, state=DISCOVERED)
                self.stopping.set()
                return
            except asyncio.CancelledError:
                raise
            except Exception as e:  # ошибка кода — не теряем документ
                self.store.update_doc(doc_id, lang, state=FAILED, error_kind="extract", error=f"internal: {e!r}")
                self.log(f"  {lang}:{doc_id}: внутренняя ошибка {e!r}")

    async def progress(self) -> None:
        last = 0.0
        while not self.stopping.is_set():
            await asyncio.sleep(1)
            if time.time() - last >= self.cfg.progress_every:
                last = time.time()
                self.print_progress()
                await self.controller.tick()

    def print_progress(self) -> None:
        now = time.time()
        win = min(600.0, now - self.t0)
        parts = []
        remaining_all = 0
        for lang in self.cfg.langs:
            c = self.store.counts(lang)
            total_found = self.store.get_meta(f"catalog_total_found_{lang}_{self.pass_no(lang)}")
            done = sum(c.get(s, 0) for s in (VALIDATED, INCOMPLETE, REVIEW, FAILED))
            remaining = max(c["_total"], total_found or 0) - done if not self.cfg.max_pages else c["_total"] - done
            remaining_all += remaining
            parts.append(
                f"{lang}: найдено {c['_total']}" + (f"/~{total_found}" if total_found else "")
                + f", готово {c.get(VALIDATED, 0)}, неполн. {c.get(INCOMPLETE, 0)}, проверка статуса {c.get(REVIEW, 0)}"
                + f", ошибки загр./извл. {c['_failed_download']}/{c['_failed_extract']}, очередь {c.get(DISCOVERED, 0)}")
        dpm = self.store.saves_since(now - win) / (win / 60) if win > 30 else 0.0
        eta = ""
        done_run = self.store.saves_since(self.t0)
        if dpm > 0 and now - self.t0 > 300 and done_run >= 20:
            eta = f" | ETA ≈ {remaining_all / dpm / 60:.1f} ч (оценка по последним {win / 60:.0f} мин)"
        self.log(" || ".join(parts)
                 + f" || параллельность {self.http.limit.limit} (в работе {self.http.limit.in_flight}), rps≤{self.http.rate.rps:.2f}"
                 + f" | {dpm:.1f} док/мин{eta}")

    # -- запуск -------------------------------------------------------------------------------
    async def crawl(self) -> int:
        self.stopping = asyncio.Event()
        rec = self.store.recover()
        if rec["requeued"] or rec["to_reparse"]:
            self.log(f"восстановление после остановки: в очередь {rec['requeued']}, на конвертацию {rec['to_reparse']}")
        for lang in self.cfg.langs:
            self.store.set_meta(f"catalog_pass_{lang}", self.pass_no(lang))
        for lang, i in self.cfg.ids:
            self.store.add_manual(i, lang, f"{BASE_URL}/{SITE_SLUG[lang]}/docs/{i}")
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, self._on_signal)
            except NotImplementedError:
                pass
        self.pool = ProcessPoolExecutor(max_workers=2)
        try:
            q = ",".join("?" * len(self.cfg.langs))
            for r in self.store.db.execute(f"SELECT doc_id, language FROM docs WHERE state IN (?,?) AND language IN ({q})",
                                           (DOWNLOADED, PARSED, *self.cfg.langs)).fetchall():
                st = await self.parse_and_save(r["doc_id"], r["language"])
                self.log(f"  {r['language']}:{r['doc_id']}: сконвертирован из сохранённого оригинала -> {st}")
            async with Http(self.cfg, self.log) as http:
                self.http = http
                robots = await http.load_robots(BASE_URL)
                self.store.set_meta("robots_txt", robots)
                self.store.set_meta("robots_checked_at", utcnow())
                for lang in self.cfg.langs:
                    if not http.allowed(page_url(1, self.cfg.pagesize, lang=lang)):
                        self.log(f"robots.txt запрещает путь каталога {lang} — остановка")
                        return 2
                if self.cfg.doc_transport == "api":
                    from urllib import robotparser
                    ra = await http.get("https://adilet.zan.kz/robots.txt", kind="robots", check_robots=False, max_attempts=3)
                    self.store.set_meta("robots_txt_api_host", ra.text())
                    rp = robotparser.RobotFileParser()
                    rp.parse(ra.text().splitlines())
                    allowed = rp.can_fetch(self.cfg.user_agent, api_url("K2600000000", "ru"))
                    msg = (f"ВНИМАНИЕ: robots.txt adilet.zan.kz {'разрешает' if allowed else 'ЗАПРЕЩАЕТ'} /api/; "
                           f"документы берутся из API по решению оператора; лимиты общие с каталогом")
                    self.log(msg)
                    self.store.event("warn", "api_robots_override", msg)
                self.controller = Controller(http, self.cfg, lambda: self.store.saves_since(0), self.log)
                self.log(f"старт: языки {','.join(self.cfg.langs)}, документы через {self.cfg.doc_transport}, "
                         f"HTTP {self.cfg.transport}, rps {self.cfg.rps} (общий), "
                         f"параллельность {self.cfg.start_concurrency} (авто до {self.cfg.max_concurrency})")
                if not self.cfg.catalog:
                    self.catalog_done = True
                tasks = [asyncio.create_task(self.worker()) for _ in range(self.cfg.max_concurrency)]
                if self.cfg.catalog:
                    tasks.append(asyncio.create_task(self.run_catalog()))
                prog = asyncio.create_task(self.progress())
                waiter = asyncio.create_task(self.stopping.wait())
                workers = asyncio.gather(*tasks, return_exceptions=True)
                done, _ = await asyncio.wait({workers, waiter}, return_when=asyncio.FIRST_COMPLETED)
                if waiter in done:
                    for t in tasks:
                        t.cancel()
                    await asyncio.gather(*tasks, return_exceptions=True)
                else:
                    for res in workers.result():
                        if isinstance(res, Blocked):
                            self.blocked = str(res)
                        elif isinstance(res, Exception):
                            self.log(f"задача завершилась с ошибкой: {res!r}")
                self.stopping.set()
                waiter.cancel()
                prog.cancel()
                self.print_progress()
                for d in self.controller.decisions:
                    self.store.event("info", "concurrency", d)
        finally:
            self.store.recover()
            if self.pool:
                self.pool.shutdown(wait=True)
        if self.blocked:
            self.log(f"СБОР ОСТАНОВЛЕН: {self.blocked}. Прогресс сохранён; продолжите позже той же командой.")
            self.store.event("error", "blocked", self.blocked)
            return 3
        return 0

    def _on_signal(self) -> None:
        if self.stopping.is_set():
            self.log("повторный сигнал — немедленный выход (незавершённые задачи вернутся в очередь при следующем запуске)")
            os._exit(130)
        self.log("остановка: незавершённые документы вернутся в очередь…")
        self.stopping.set()

    async def reconvert(self, ids: list[tuple[str, str]] | None, states: list[str], langs: tuple = LANGS) -> dict:
        self.pool = ProcessPoolExecutor(max_workers=max(1, (os.cpu_count() or 2) - 1))
        try:
            if ids:
                rows = [r for r in (self.store.doc(i, l) for l, i in ids) if r]
            else:
                qs, ql = ",".join("?" * len(states)), ",".join("?" * len(langs))
                rows = self.store.db.execute(f"SELECT * FROM docs WHERE state IN ({qs}) AND language IN ({ql})",
                                             (*states, *langs)).fetchall()
            rows = [r for r in rows if self.paths.raw_doc(r["doc_id"], r["language"]).exists()
                    or self.paths.raw_api(r["doc_id"], r["language"]).exists()]
            out: dict = {}
            sem = asyncio.Semaphore((os.cpu_count() or 2) * 2)

            async def one(r):
                async with sem:
                    st = await self.parse_and_save(r["doc_id"], r["language"])
                    key = f"{r['language']}:{st}"
                    out[key] = out.get(key, 0) + 1

            await asyncio.gather(*(one(r) for r in rows))
            return out
        finally:
            self.pool.shutdown(wait=True)
