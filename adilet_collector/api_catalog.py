"""Каталог через POST https://adilet.zan.kz/api/search (все статусы, ru/kk).

Проверено 17.10.2026:
* page_size=100 (200 и 1000 отклоняются); repeat страницы 1 даёт тот же порядок; aggregations_enabled=false
  не меняет выдачу и total_hits; total_pages в ответе ненадёжен — считаем ceil(total_hits/100).
* Предел глубины: page*page_size <= 10000 (HTTP 422 PageWindowExceeded), страница 100 уже пустая ->
  каталог режется на срезы фильтром filters.date_from/date_to (фактически по start_date; принимает
  ISO-время). Диапазон 0001..2999 возвращает все документы языка (без даты нет).
* Фильтр статуса НЕ используется: собираются все статусы.
"""
from __future__ import annotations

import hashlib
import json
import math
import re
from datetime import datetime, timedelta

SEARCH_URL = "https://adilet.zan.kz/api/search"
PAGE_SIZE = 100
MAX_WINDOW = 10000                 # из ответа API: max_window
MAX_REACHABLE = MAX_WINDOW - PAGE_SIZE  # страница 100 пустая: фактически доступно 9 900
SORTS_CONFIRMED = ("relevance", "start_date_asc", "start_date_desc", "adoption_date_asc", "publication_date_asc")
ROOT_FROM = "0001-01-01T00:00:00Z"
ROOT_TO = "2999-12-31T23:59:59Z"
RE_ID = re.compile(r"^[A-Za-z0-9_]{5,20}$")


class SearchError(Exception):
    def __init__(self, kind: str, detail: str):
        super().__init__(f"{kind}: {detail}")
        self.kind = kind
        self.detail = detail


STATUSES_CONFIRMED = ("new", "upd", "err", "yts", "stp")  # из клиентского кода; фильтр status проверен (err = 701)


def search_body(lang_slug: str, page: int, sort: str, date_from: str | None = None, date_to: str | None = None,
                status: str | None = None) -> dict:
    filters = {"language": lang_slug}
    if status:
        filters["status"] = status  # только для дробления плотного кластера одной даты; объединение покрывает всё
    if date_from:
        filters["date_from"] = date_from
    if date_to:
        filters["date_to"] = date_to
    return {"query": "", "filters": filters, "page": page, "page_size": PAGE_SIZE, "sort": sort,
            "aggregations_enabled": False, "enable_fuzzy": False, "enable_highlighting": False}


def parse_search(raw: bytes, lang_slug: str, page: int) -> dict:
    try:
        d = json.loads(raw.decode("utf-8", errors="replace"))
    except ValueError:
        raise SearchError("bad_json", "ответ поиска не JSON")
    if not isinstance(d, dict) or not isinstance(d.get("results"), list):
        raise SearchError("no_results_field", f"нет results: {str(d)[:200]}")
    if d.get("page") not in (page, None):
        raise SearchError("wrong_page", f"запрошена {page}, получена {d.get('page')}")
    if d.get("page_size") not in (PAGE_SIZE, None):
        raise SearchError("wrong_page_size", f"page_size={d.get('page_size')}")
    total = d.get("total_hits")
    if not isinstance(total, int):
        raise SearchError("no_total", "нет total_hits")
    recs, warnings = [], []
    for r in d["results"]:
        gid = r.get("group_number")
        if not gid or not RE_ID.match(gid):
            warnings.append(f"bad_group_number:{gid!r}")
            continue
        if r.get("language") != lang_slug:
            raise SearchError("wrong_language", f"{gid}: language={r.get('language')!r}, ожидался {lang_slug}")
        recs.append(r)
    ids = [r["group_number"] for r in recs]
    if len(set(ids)) != len(ids):
        warnings.append(f"duplicate_ids_on_page:{len(ids) - len(set(ids))}")
    return {"records": recs, "total_hits": total, "ids": ids,
            "ids_hash": hashlib.sha256("\n".join(ids).encode()).hexdigest(),
            "total_pages_field": d.get("total_pages"), "took_ms": d.get("took_ms"), "warnings": warnings}


def n_pages(total: int) -> int:
    return max(1, math.ceil(total / PAGE_SIZE))


def _ts(s: str) -> datetime:
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


def _fmt(d: datetime) -> str:
    return f"{d.year:04d}-{d.month:02d}-{d.day:02d}T{d.hour:02d}:{d.minute:02d}:{d.second:02d}Z"  # strftime %Y платформозависим для года < 1000


def slice_id(date_from: str, date_to: str) -> str:
    return re.sub(r"[-:]", "", date_from) + "_" + re.sub(r"[-:]", "", date_to)


def splittable(date_from: str, date_to: str) -> bool:
    return (_ts(date_to) - _ts(date_from)).total_seconds() >= 2


def split(date_from: str, date_to: str) -> list[tuple[str, str]]:
    """Делит диапазон пополам. Границы перекрываются в одной секунде (семантика включения границы
    у API не документирована) — дубликаты убираются по ID, покрытие проверяется по total_hits."""
    a, b = _ts(date_from), _ts(date_to)
    mid = a + timedelta(seconds=int((b - a).total_seconds() // 2))
    return [(_fmt(a), _fmt(mid)), (_fmt(mid), _fmt(b))]
