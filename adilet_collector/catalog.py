"""Страницы каталога: адреса, разбор карточек, проверки целостности выдачи.

/rus/search/ запрещён в robots.txt, поэтому используется разрешённый /rus/index/docs/
с теми же параметрами в пути (проверено: та же выдача, «Статус ( Действующий )», 133 203 док.).
Сортировка по дате принятия по возрастанию: новые акты добавляются в конец, ранние страницы не сдвигаются.
"""
from __future__ import annotations

import hashlib
import re
from urllib.parse import parse_qsl, unquote, urljoin, urlsplit

from bs4 import BeautifulSoup

from .document import BASE_URL, BLOCKED_RE, norm_ws

DEFAULT_FILTER = "st=new%7Cupd"
RE_DOC_HREF = re.compile(r"^/(?:rus|kaz)/docs/([A-Za-z0-9_]+)/?$")

# Проверено на сайте 17.10.2026: /kaz/search/ запрещён robots.txt так же, как /rus/search/,
# а /kaz/index/docs/ разрешён и отдаёт фильтрованную выдачу (130 853 док., 1 309 стр.).
LANG_RULES = {
    "ru": {"slug": "rus", "filter_label": "Действующий", "sort_label": "дате принятия",
           "found": re.compile(r"Найдено:\s*([\d\s]+)"),
           "pages": re.compile(r"Страница\s+(\d+)\s+из\s+(\d+)"), "pages_total_group": 2},
    "kk": {"slug": "kaz", "filter_label": "Қолданылып жүрген", "sort_label": "қабылдау күні",
           "found": re.compile(r"Табылды:\s*([\d\s]+)"),
           "pages": re.compile(r"(\d+)\s+беттен\s+(\d+)\s+бет"), "pages_total_group": 1},
}


def page_url(page: int, pagesize: int = 100, filt: str = DEFAULT_FILTER, sort: bool = True, lang: str = "ru") -> str:
    parts = [filt, f"pagesize={pagesize}"]
    if sort:
        parts += ["sort_field=dt", "sort_desc=false"]
    parts.append(f"page={page}")
    return f"{BASE_URL}/{LANG_RULES[lang]['slug']}/index/docs/" + "&".join(parts)


class CatalogError(Exception):
    def __init__(self, kind: str, detail: str):
        super().__init__(f"{kind}: {detail}")
        self.kind = kind


def _href_params(href: str) -> dict:
    path = unquote(urlsplit(href).path)
    tail = path.split("/docs/", 1)[-1]
    return dict(parse_qsl(tail.replace("&amp;", "&"), keep_blank_values=True)) if "=" in tail else {}


def parse_page(html_text: str, url: str, requested_page: int, pagesize: int, sort: bool = True, lang: str = "ru") -> dict:
    rules = LANG_RULES[lang]
    soup = BeautifulSoup(html_text, "lxml")
    serp = soup.select_one(".serp")
    cards = soup.select(".serp .post_holder")
    if serp is None and BLOCKED_RE.search(html_text):
        raise CatalogError("blocked", "CAPTCHA/блокировка вместо каталога")
    if serp is None:
        raise CatalogError("not_catalog", "нет блока .serp — получена не страница каталога")

    warnings: list[str] = []
    cur = soup.select_one(".wp-pagenavi .current")
    current = int(cur.get_text(strip=True)) if cur and cur.get_text(strip=True).isdigit() else None
    if current != requested_page:
        raise CatalogError("wrong_page", f"запрошена страница {requested_page}, сайт вернул {current}")

    crumbs = norm_ws(soup.select_one(".breadcrumbs").get_text(" ")) if soup.select_one(".breadcrumbs") else ""
    if rules["filter_label"] not in crumbs:
        raise CatalogError("filter_not_applied", f"в хлебных крошках нет фильтра статуса: {crumbs[:200]!r}")
    m = rules["found"].search(crumbs)
    total_found = int(re.sub(r"\D", "", m.group(1))) if m else None

    if sort:
        sm = soup.select_one(".sortmode .sortmode-current")
        if sm is None or rules["sort_label"] not in sm.get_text():
            raise CatalogError("sort_not_applied", f"сортировка не применилась: {sm.get_text(strip=True) if sm else None!r}")

    pages_txt = soup.select_one(".wp-pagenavi .pages")
    total_pages = None
    if pages_txt:
        mm = rules["pages"].search(norm_ws(pages_txt.get_text(" ")))
        if mm:
            total_pages = int(mm.group(rules["pages_total_group"]))
    size_el = soup.select_one(".wp-pagenavi .pages #pagesizelink")
    if size_el is not None and size_el.get_text(strip=True) != str(pagesize):
        raise CatalogError("pagesize_not_applied", f"размер выдачи {size_el.get_text(strip=True)} вместо {pagesize}")

    # nextpostslink встречается дважды: «>» (следующая) и «»» (последняя) — различаем по тексту и номеру.
    next_page = last_page = None
    for a in soup.select(".wp-pagenavi a.nextpostslink"):
        p = _href_params(a.get("href", ""))
        try:
            num = int(p.get("page", ""))
        except ValueError:
            continue
        label = a.get_text(strip=True)
        if label in (">", "›") or (current is not None and num == current + 1 and label not in ("»",)):
            next_page = num
        if label in ("»",):
            last_page = num
    if last_page is not None and total_pages is not None and last_page != total_pages:
        warnings.append(f"last_link_mismatch:{last_page}!={total_pages}")
    if total_pages is None:
        total_pages = last_page or (current if next_page is None else None)
    if next_page is not None and next_page != (current or 0) + 1:
        raise CatalogError("pagination_loop", f"ссылка «следующая» ведёт на {next_page} со страницы {current}")
    if total_pages and current is not None and current < total_pages and next_page is None:
        raise CatalogError("pagination_broken", "нет ссылки на следующую страницу до конца каталога")

    out = []
    for c in cards:
        a = c.select_one(".post_header a")
        if a is None or not a.get("href"):
            warnings.append("card_without_link")
            continue
        href = a["href"].strip()
        absu = urljoin(url, href)
        mm = RE_DOC_HREF.match(urlsplit(absu).path)
        if mm and not urlsplit(absu).path.startswith(f"/{rules['slug']}/docs/"):
            raise CatalogError("wrong_language_links", f"карточка ведёт на {href}, ожидался /{rules['slug']}/docs/")
        if not mm:
            warnings.append(f"unexpected_href:{href}")
            continue
        st = c.select_one(".status")
        cls = [x for x in (st.get("class") or []) if x != "status"] if st else []
        p = c.find("p")
        out.append({
            "doc_id": mm.group(1),
            "url": absu,
            "title": norm_ws(a.get_text()),
            "status_raw": norm_ws(st.get_text()) if st else None,
            "status_class": cls[0] if cls else None,
            "status_class_full": " ".join(st.get("class") or []) if st else None,
            "requisites": norm_ws(p.get_text()) if p else None,
        })
    if not out:
        raise CatalogError("empty_page", "страница без карточек (не считается концом каталога)")
    if total_pages and current is not None and current < total_pages and len(out) != pagesize:
        warnings.append(f"short_page:{len(out)}")
    if total_pages and current == total_pages and total_found is not None:
        expected = total_found - (total_pages - 1) * pagesize
        if expected != len(out):
            warnings.append(f"last_page_count:{len(out)}!={expected}")
    ids = [x["doc_id"] for x in out]
    if len(set(ids)) != len(ids):
        warnings.append(f"duplicate_ids_on_page:{len(ids) - len(set(ids))}")
    return {
        "cards": out,
        "current": current,
        "total_pages": total_pages,
        "total_found": total_found,
        "next_page": next_page,
        "last_page": last_page,
        "ids_hash": hashlib.sha256("\n".join(ids).encode()).hexdigest(),
        "warnings": warnings,
    }
