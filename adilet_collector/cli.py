"""Командная строка: python -m adilet_collector <команда> ..."""
from __future__ import annotations

import argparse
import asyncio
import fcntl
import json
import sys
import time
from datetime import datetime
from pathlib import Path

from .pipeline import Collector, Config, parse_ids
from .store import DISCOVERED, FAILED, INCOMPLETE, LANGS, REVIEW, VALIDATED, Paths, Store, utcnow


def _langs(s: str) -> tuple:
    langs = tuple(x.strip() for x in s.split(",") if x.strip())
    bad = [l for l in langs if l not in LANGS]
    if bad:
        sys.exit(f"неизвестный язык: {bad}; доступны {LANGS}")
    return langs


def _lock(paths: Paths):
    paths.root.mkdir(parents=True, exist_ok=True)
    f = open(paths.lock, "w")
    try:
        fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        sys.exit("Другой экземпляр сборщика уже работает с этим каталогом данных — второй запуск запрещён "
                 "(лимиты нагрузки должны быть общими).")
    f.write(str(time.time()))
    f.flush()
    return f


def _cfg(a) -> Config:
    langs = _langs(a.langs)
    return Config(
        data_dir=Path(a.data_dir).resolve(), langs=langs, rps=a.rps, start_concurrency=a.start_concurrency,
        max_concurrency=a.max_concurrency, transport=a.transport, user_agent=a.user_agent, timeout=a.timeout,
        adapt_window=a.adapt_window, max_pages=getattr(a, "max_pages", None), max_docs=getattr(a, "max_docs", None),
        catalog=not getattr(a, "no_catalog", False), ids=tuple(parse_ids(getattr(a, "ids", None), langs)),
        with_info=a.with_info, fetch_attachments=a.fetch_attachments,
        attachments_ignore_robots=a.attachments_ignore_robots, publish_incomplete=a.publish_incomplete,
        doc_transport=a.doc_transport, api_fallback=not a.no_api_fallback, verbose=a.verbose)


def cmd_crawl(a) -> int:
    if a.doc_transport == "api" and not a.i_accept_api_robots_risk:
        sys.exit("--doc-transport api: robots.txt adilet.zan.kz запрещает /api/. Запуск только с явным флагом "
                 "--i-accept-api-robots-risk (решение оператора).")
    if a.max_concurrency > 8 and not a.i_measured_above_8:
        sys.exit("--max-concurrency > 8 требует флага --i-measured-above-8 (подтверждение отдельных измерений).")
    cfg = _cfg(a)
    lock = _lock(Paths(cfg.data_dir))
    return asyncio.run(Collector(cfg).crawl())


def cmd_reconvert(a) -> int:
    langs = _langs(a.langs)
    cfg = Config(data_dir=Path(a.data_dir).resolve(), langs=langs, publish_incomplete=a.publish_incomplete)
    lock = _lock(Paths(cfg.data_dir))
    col = Collector(cfg)
    ids = parse_ids(a.ids, langs) or None
    t = time.time()
    out = asyncio.run(col.reconvert(ids, a.states.split(","), langs))
    print(f"переконвертировано без сети за {time.time() - t:.1f} с: {out}")
    return 0


def cmd_retry(a) -> int:
    paths = Paths(Path(a.data_dir).resolve())
    lock = _lock(paths)
    st = Store(paths)
    langs = _langs(a.langs)
    q = ",".join("?" * len(langs))
    if a.kind in ("download", "all"):
        n = st.db.execute(f"UPDATE docs SET state=?, attempts=0, next_try_at=0 WHERE state=? AND error_kind='download' "
                          f"AND language IN ({q})", (DISCOVERED, FAILED, *langs)).rowcount
        st.db.execute(f"UPDATE docs SET next_try_at=0 WHERE state=? AND language IN ({q})", (DISCOVERED, *langs))
        print(f"документов с ошибкой загрузки возвращено в очередь: {n}. Запустите crawl для повторной загрузки.")
    if a.kind in ("catalog", "all"):
        c = st.db.execute(f"UPDATE catalog_pages SET status='pending', attempts=0 WHERE status!='done' AND language IN ({q})",
                          langs).rowcount
        print(f"страниц каталога возвращено в очередь: {c}")
    if a.kind in ("extract", "all"):
        col = Collector(Config(data_dir=paths.root, langs=langs))
        out = asyncio.run(col.reconvert(None, [FAILED], langs))
        print(f"ошибки извлечения переконвертированы из оригиналов: {out}")
    return 0


def cmd_recheck(a) -> int:
    paths = Paths(Path(a.data_dir).resolve())
    lock = _lock(paths)
    st = Store(paths)
    for lang in _langs(a.langs):
        cur = st.get_meta(f"catalog_pass_{lang}", 1)
        st.set_meta(f"catalog_pass_{lang}", cur + 1)
        print(f"{lang}: начат проход сверки каталога №{cur + 1}")
    print("Запустите crawl: новые ID будут добавлены, проверенные документы не скачиваются повторно.")
    return 0


def collect_report(paths: Paths, lang: str) -> dict:
    st = Store(paths)
    db = st.db
    c = st.counts(lang)
    pass_no = st.get_meta(f"catalog_pass_{lang}", 1)

    def rows(where, args=()):
        return [dict(r) for r in db.execute(
            f"SELECT doc_id, state, error_kind, error, card_status_raw, page_status_raw, n_attachments FROM docs "
            f"WHERE language=? AND ({where}) ORDER BY doc_id", (lang, *args))]

    pages = {r[0]: r[1] for r in db.execute(
        "SELECT status, COUNT(*) FROM catalog_pages WHERE language=? AND pass=? GROUP BY status", (lang, pass_no))}
    return {
        "language": lang,
        "generated_at": utcnow(),
        "catalog": {
            "pass": pass_no,
            "site_total_found": st.get_meta(f"catalog_total_found_{lang}_{pass_no}"),
            "site_total_pages": st.get_meta(f"catalog_total_pages_{lang}_{pass_no}"),
            "pages": pages,
            "complete_at": st.get_meta(f"catalog_complete_{lang}_{pass_no}"),
            "unique_ids_in_db": c["_total"],
            "not_seen_in_this_pass": [r[0] for r in db.execute(
                "SELECT doc_id FROM docs WHERE language=? AND discovered_from LIKE '%catalog%' AND last_seen_pass<? LIMIT 1000",
                (lang, pass_no))],
            "catalog_warnings": [dict(r) for r in db.execute(
                "SELECT ts, message FROM events WHERE language=? AND kind IN ('catalog_page','catalog_changed') "
                "ORDER BY ts DESC LIMIT 200", (lang,))],
        },
        "counts": c,
        "fully_extracted": c.get(VALIDATED, 0),
        "unread_attachments": rows("state=?", (INCOMPLETE,)),
        "download_errors": rows("state=? AND error_kind='download'", (FAILED,)),
        "extraction_errors": rows("state=? AND error_kind='extract'", (FAILED,)),
        "status_discrepancies": rows("state=? OR status_mismatch=1", (REVIEW,)),
    }


def cmd_report(a) -> int:
    paths = Paths(Path(a.data_dir).resolve())
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    paths.reports.mkdir(parents=True, exist_ok=True)
    reps = {l: collect_report(paths, l) for l in _langs(a.langs)}
    st = Store(paths)
    common = {
        "concurrency_decisions": [dict(r) for r in st.db.execute("SELECT ts, message FROM events WHERE kind='concurrency' ORDER BY ts")],
        "blocks": [dict(r) for r in st.db.execute("SELECT ts, message FROM events WHERE kind='blocked' ORDER BY ts")],
        "migrations": [dict(r) for r in st.db.execute("SELECT ts, message FROM events WHERE kind='migration' ORDER BY ts")],
    }
    jp = paths.reports / f"report_{ts}.json"
    jp.write_text(json.dumps({"languages": reps, **common}, ensure_ascii=False, indent=2), encoding="utf-8")
    lines = [f"# Отчёт о полноте сбора adilet ({utcnow()})", ""]
    for lang, rep in reps.items():
        c, cat = rep["counts"], rep["catalog"]
        lines += [
            f"## Язык {lang}", "",
            f"Каталог: проход {cat['pass']}, на сайте {cat['site_total_found']} док. / {cat['site_total_pages']} стр.; "
            f"страниц: {cat['pages']}; уникальных ID: {cat['unique_ids_in_db']}; полностью пройден: {cat['complete_at'] or 'нет'}", "",
            f"- Полностью извлечены (data/adilet_{lang}): {rep['fully_extracted']}",
            f"- С непрочитанными содержательными приложениями (data/incomplete/{lang}): {len(rep['unread_attachments'])}",
            f"- Ошибки загрузки: {len(rep['download_errors'])}",
            f"- Ошибки извлечения (quarantine): {len(rep['extraction_errors'])}",
            f"- Расхождения/недействующий статус (review, quarantine): {len(rep['status_discrepancies'])}",
            f"- В очереди: {c.get(DISCOVERED, 0)}", "",
        ]
        for title, key in (("Ошибки извлечения", "extraction_errors"), ("Ошибки загрузки", "download_errors"),
                           ("Расхождения статусов", "status_discrepancies"),
                           ("Непрочитанные приложения", "unread_attachments")):
            items = rep[key]
            if items:
                lines.append(f"### {title} ({len(items)})")
                for r in items[:50]:
                    lines.append(f"- {r['doc_id']}: {r['error'] or ''} карточка={r['card_status_raw']} "
                                 f"страница={r['page_status_raw']} приложений={r['n_attachments']}")
                if len(items) > 50:
                    lines.append(f"- … полный список в {jp.name}")
                lines.append("")
    if common["concurrency_decisions"]:
        lines.append("## Решения по параллельности")
        lines += [f"- {d['ts']}: {d['message']}" for d in common["concurrency_decisions"][-30:]]
    mp = paths.reports / f"report_{ts}.md"
    mp.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print("\n".join(lines))
    print(f"\nотчёт: {mp}\nполные списки: {jp}")
    return 0


def cmd_stats(a) -> int:
    paths = Paths(Path(a.data_dir).resolve())
    st = Store(paths)
    out = {}
    for lang in _langs(a.langs):
        p = st.get_meta(f"catalog_pass_{lang}", 1)
        out[lang] = {
            "counts": st.counts(lang),
            "catalog_pass": p,
            "site_total_found": st.get_meta(f"catalog_total_found_{lang}_{p}"),
            "catalog_pages": {r[0]: r[1] for r in st.db.execute(
                "SELECT status, COUNT(*) FROM catalog_pages WHERE language=? AND pass=? GROUP BY status", (lang, p))},
            "saved_last_hour": st.saves_since(time.time() - 3600, lang),
        }
    print(json.dumps(out, ensure_ascii=False, indent=2))
    return 0


def main(argv=None) -> int:
    p = argparse.ArgumentParser(prog="adilet_collector", description="Сбор законодательства РК с old.adilet.zan.kz (ru + kk)")
    p.add_argument("--data-dir", default="data")
    sub = p.add_subparsers(dest="cmd", required=True)

    def lang_arg(sp):
        sp.add_argument("--langs", default="ru,kk", help="языки через запятую: ru,kk")

    def net_args(sp):
        lang_arg(sp)
        sp.add_argument("--rps", type=float, default=2.0, help="глобальный лимит запросов/с на домен (общий для всех языков)")
        sp.add_argument("--start-concurrency", type=int, default=2)
        sp.add_argument("--max-concurrency", type=int, default=8, help="потолок авто-повышения (выше 8 — только явно)")
        sp.add_argument("--i-measured-above-8", action="store_true", help=argparse.SUPPRESS)
        sp.add_argument("--adapt-window", type=float, default=120.0, help="минимальное окно оценки уровня, с")
        sp.add_argument("--transport", choices=["aiohttp", "curl"], default="aiohttp")
        sp.add_argument("--user-agent", default="Mozilla/5.0 (compatible; TrafficLawAdiletCollector/1.0)")
        sp.add_argument("--timeout", type=float, default=180.0)
        sp.add_argument("--with-info", action="store_true", help="также вкладка «Информация» (+1 запрос на документ)")
        sp.add_argument("--fetch-attachments", action="store_true", help="скачивать приложения, разрешённые robots.txt")
        sp.add_argument("--attachments-ignore-robots", action="store_true",
                        help="скачивать приложения из /files/, запрещённого robots.txt (только по решению оператора)")
        sp.add_argument("--publish-incomplete", action="store_true",
                        help="документы с непрочитанными приложениями класть в adilet_<lang> (по умолчанию — data/incomplete/<lang>)")
        sp.add_argument("--doc-transport", choices=["old_html", "api"], default="old_html",
                        help="откуда брать документы: страницы old.adilet (по умолчанию) или API adilet.zan.kz")
        sp.add_argument("--i-accept-api-robots-risk", action="store_true",
                        help="подтверждение оператора: API запрещён robots.txt нового домена")
        sp.add_argument("--no-api-fallback", action="store_true",
                        help="не переходить на страницу старого сайта при ошибке/заглушке API")
        sp.add_argument("--verbose", action="store_true")

    c = sub.add_parser("crawl", help="полный сбор / продолжение после остановки")
    net_args(c)
    c.add_argument("--max-pages", type=int, help="на каждый язык")
    c.add_argument("--max-docs", type=int)
    c.add_argument("--no-catalog", action="store_true", help="только очередь документов, без обхода каталогов")
    c.add_argument("--ids", help="ID через запятую; «kk:ID» — только для языка, без префикса — для всех")
    c.set_defaults(fn=cmd_crawl)

    pl = sub.add_parser("pilot", help="пробный сбор: 2 страницы каталога на язык, до 20 документов")
    net_args(pl)
    pl.add_argument("--max-pages", type=int, default=2)
    pl.add_argument("--max-docs", type=int, default=20)
    pl.add_argument("--ids")
    pl.set_defaults(fn=cmd_crawl)

    r = sub.add_parser("reconvert", help="повторная конвертация сохранённых HTML без сети")
    lang_arg(r)
    r.add_argument("--ids")
    r.add_argument("--states", default="validated,incomplete,review,failed,downloaded,parsed")
    r.add_argument("--publish-incomplete", action="store_true")
    r.set_defaults(fn=cmd_reconvert)

    rf = sub.add_parser("retry-failed", help="повтор ошибок")
    lang_arg(rf)
    rf.add_argument("--kind", choices=["download", "extract", "catalog", "all"], default="all")
    rf.set_defaults(fn=cmd_retry)

    rc = sub.add_parser("recheck-catalog", help="новый проход сверки каталогов (поиск новых ID)")
    lang_arg(rc)
    rc.set_defaults(fn=cmd_recheck)

    s = sub.add_parser("stats", help="статистика по языкам")
    lang_arg(s)
    s.set_defaults(fn=cmd_stats)
    rp = sub.add_parser("report", help="отчёт о полноте по языкам (data/reports/)")
    lang_arg(rp)
    rp.set_defaults(fn=cmd_report)

    a = p.parse_args(argv)
    return a.fn(a) or 0


if __name__ == "__main__":
    sys.exit(main())
