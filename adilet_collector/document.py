"""Разбор страницы документа adilet: реквизиты, статус, текст -> Markdown, проверка полноты.

Конвертация детерминированная: никакого пересказа или исправления текста. Проверка
полноты независима от Markdown-генератора: текст исходного контейнера сравнивается
с текстом, извлечённым обратно из готового Markdown.
"""
from __future__ import annotations

import copy
import hashlib
import html as html_lib
import re
import unicodedata
from collections import Counter
from dataclasses import dataclass, field
from urllib.parse import urljoin, urlsplit

from bs4 import BeautifulSoup, Comment, NavigableString, Tag

BASE_URL = "https://old.adilet.zan.kz"

# ---------------------------------------------------------------------------
# Нормализация (только технические различия)
# ---------------------------------------------------------------------------
_SPACE_CHARS = "            　\t\r\f\v"
_DROP_CHARS = "\u200b\u200c\u200d\u2060\ufeff\u00ad\x0e\x0f"
# Внутренние маркеры тегов <sup>/<sub>: html.escape их не трогает, при сборке -> < >
TAG_OPEN, TAG_CLOSE = "\x0e", "\x0f"
_SPACE_TABLE = {ord(c): " " for c in _SPACE_CHARS}
_SPACE_TABLE.update({ord(c): None for c in _DROP_CHARS})


def norm_chars(s: str) -> str:
    """NFC (не NFKC: NFKC превратил бы ³ в 3), неразрывные пробелы -> пробел."""
    return unicodedata.normalize("NFC", s).translate(_SPACE_TABLE)


def norm_ws(s: str) -> str:
    return re.sub(r"\s+", " ", norm_chars(s)).strip()


# Надстрочные/подстрочные символы: 128<sup>3</sup> -> 128³ (а не 1283).
_SUP = dict(zip("0123456789+-=()−", "⁰¹²³⁴⁵⁶⁷⁸⁹⁺⁻⁼⁽⁾⁻"))
_SUB = dict(zip("0123456789+-=()−", "₀₁₂₃₄₅₆₇₈₉₊₋₌₍₎₋"))

BLOCK_TAGS = {
    "p", "div", "h1", "h2", "h3", "h4", "h5", "h6", "table", "ul", "ol", "li",
    "blockquote", "center", "pre", "hr", "section", "article", "dl", "dt", "dd",
    "tr", "td", "th", "tbody", "thead", "tfoot", "caption", "address", "form",
}
HEADING_TAGS = {"h1", "h2", "h3", "h4", "h5", "h6"}
REMOVE_TAGS = {"script", "style", "noscript", "iframe", "ins", "form", "button",
               "input", "select", "textarea", "object", "embed", "svg", "link", "meta"}

# Русские и казахские обозначения не пересекаются, поэтому шаблоны общие для обоих языков.
# Казахские: номер впереди — «І бөлім», «1-тарау», «1-бап», «…қаулысына 1-қосымша».
_KK_I = "[ІIі]"
_NUM = r"[\dIVXLCІ]+"
RE_PART = re.compile(r"^(?:ОБЩАЯ|ОСОБЕННАЯ|Общая|Особенная)\s+(?:ЧАСТЬ|часть)\b"
                     r"|^(?:ЖАЛПЫ|ЕРЕКШЕ|Жалпы|Ерекше)\s+(?:БӨЛ[ІI]К|бөл[іi]к)(?:\s|$)")
RE_SECTION = re.compile(r"^(?:РАЗДЕЛ|Раздел)\s+\S|^" + _NUM + r"(?:[-–]\s*|\s+)(?:БӨЛ[ІI]М|[Бб]өл[іi]м)(?:\b|[.\s]|$)")
RE_CHAPTER = re.compile(r"^(?:ГЛАВА|Глава)\s+\S|^\d+(?:[-–]\d+)?[-–]\s*(?:ТАРАУ|[Тт]арау)(?:[.\s]|$)")
RE_ARTICLE = re.compile(r"^(?:СТАТЬЯ|Статья)\s+\d|^\d[\d⁰¹²³⁴⁵⁶⁷⁸⁹\-–]*[-–]\s*(?:БАП|бап)(?:[.\s]|$)")
RE_APPENDIX = re.compile(r"^(?:ПРИЛОЖЕНИЕ|Приложение|ҚОСЫМША|Қосымша)(?:\s|$)")
RE_APPENDIX_END = re.compile(r"(?:^|\s)(?:\d+(?:[-–]\d+)?[-–])?(?:ҚОСЫМША|қосымша)\.?$")
RE_NUMBERED_SECTION = re.compile(r"^\d+(?:-\d+)?\.\s+\S")  # <h3>1. Общие положения</h3>
RE_SHORT_LABEL = re.compile(r"^(?:РАЗДЕЛ|Раздел|ГЛАВА|Глава|ЧАСТЬ|Часть)\s+[\w\-–.]+\.?$"
                            r"|^" + _NUM + r"(?:[-–]\s*|\s+)(?:БӨЛ[ІI]М|[Бб]өл[іi]м|ТАРАУ|[Тт]арау)\.?$")
RE_ARTICLE_LABEL = re.compile(r"^(?:СТАТЬЯ|Статья)\s+\d[\d⁰¹²³⁴⁵⁶⁷⁸⁹\-–.]*|^\d[\d⁰¹²³⁴⁵⁶⁷⁸⁹\-–]*[-–]\s*(?:БАП|бап)")

ATTACH_EXT = re.compile(r"\.(pdf|docx?|rtf|xlsx?|zip|rar|7z|jpe?g|png|gif|tiff?|bmp|odt|ods)(?:$|\?)", re.I)

BLOCKED_RE = re.compile(r"captcha|recaptcha|hcaptcha|cf-chl|challenge-platform|Access denied|Доступ\s+(?:запрещ|огранич)", re.I)
UI_LEFTOVERS = ["adfox", "Ya.adfoxCode", "yaContextCb", "Поделиться в", "Добавить в Twitter",
                "Расширенный поиск", "Полноэкранный режим", "Сбросить"]


def abs_url(href: str, page_url: str) -> str:
    return urljoin(page_url, href.strip())


def md_url(u: str) -> str:
    return u.replace(" ", "%20").replace("(", "%28").replace(")", "%29")


def _escape_link_text(t: str) -> str:
    return t.replace("\\", "\\\\").replace("[", "\\[").replace("]", "\\]")


# ---------------------------------------------------------------------------
# Предобработка контейнера (общая для конвертера и проверки)
# ---------------------------------------------------------------------------
def preprocess_container(el: Tag) -> tuple[Tag, list[str]]:
    """Копия контейнера без скриптов/рекламы; sup/sub с цифрами -> Unicode."""
    el = copy.copy(el)
    warnings: list[str] = []
    for c in el.find_all(string=lambda s: isinstance(s, Comment)):
        c.extract()
    for t in el.find_all(list(REMOVE_TAGS)):
        if t.name in {"iframe", "object", "embed"}:
            warnings.append(f"embedded_{t.name}_removed")
        t.decompose()
    for t in el.find_all(True):
        if t.attrs is None:
            continue
        tid = str(t.get("id") or "")
        cls = " ".join(t.get("class") or [])
        if tid.startswith("adfox") or "adfox" in cls or "banner" in cls:
            t.decompose()
    for tag_name, table in (("sup", _SUP), ("sub", _SUB)):
        for t in el.find_all(tag_name):
            raw = norm_ws(t.get_text())
            if raw and all(ch in table for ch in raw) and not t.find(["a", "img"]):
                t.replace_with(NavigableString("".join(table[ch] for ch in raw)))
    return el, sorted(set(warnings))


# ---------------------------------------------------------------------------
# Конвертер в Markdown
# ---------------------------------------------------------------------------
@dataclass
class Block:
    kind: str  # para | heading | table | list
    text: str
    level: int = 0
    is_h: bool = False


class Converter:
    def __init__(self, page_url: str):
        self.page_url = page_url
        self.media: list[dict] = []
        self.links: list[dict] = []
        self.warnings: list[str] = []

    # -- inline -------------------------------------------------------------
    def inline(self, node) -> str:
        if isinstance(node, Comment):
            return ""
        if isinstance(node, NavigableString):
            return re.sub(r"\s+", " ", norm_chars(str(node)))
        if not isinstance(node, Tag):
            return ""
        name = node.name
        if name == "br":
            return "\n"
        if name == "img":
            src = node.get("src")
            if not src:
                return ""
            u = abs_url(src, self.page_url)
            self.media.append({"kind": "image", "url": u, "alt": node.get("alt") or None,
                               "width": node.get("width"), "height": node.get("height")})
            alt = norm_ws(node.get("alt") or "")
            return f"![{_escape_link_text(alt)}]({md_url(u)})"
        inner = "".join(self.inline(c) for c in node.children)
        if name == "a":
            href = (node.get("href") or "").strip()
            text = re.sub(r"\s*\n\s*", " ", inner)
            if not text.strip():
                return inner if "![" in inner else text
            if not href or href.lower().startswith(("javascript:", "mailto:", "tel:")):
                return text
            u = abs_url(href, self.page_url)
            self.links.append({"url": u, "text": norm_ws(text)})
            lead = text[: len(text) - len(text.lstrip())]
            trail = text[len(text.rstrip()):]
            return f"{lead}[{_escape_link_text(text.strip())}]({md_url(u)}){trail}"
        if name in ("sup", "sub"):
            t = inner.strip()
            return f"{TAG_OPEN}{name}{TAG_CLOSE}{t}{TAG_OPEN}/{name}{TAG_CLOSE}" if t else ""
        return inner

    # -- blocks -------------------------------------------------------------
    @staticmethod
    def _is_blockish(node) -> bool:
        return isinstance(node, Tag) and (node.name in BLOCK_TAGS or node.find(list(BLOCK_TAGS)) is not None)

    def blocks(self, container: Tag, ctx: dict | None = None) -> list[Block]:
        out: list[Block] = []
        self._walk(container, out, ctx or {})
        return out

    def _walk(self, container: Tag, out: list[Block], ctx: dict) -> None:
        run: list = []
        for child in list(container.children):
            if self._is_blockish(child):
                self._flush_run(run, out, ctx, container)
                run = []
                self._block(child, out, ctx)
            else:
                run.append(child)
        self._flush_run(run, out, ctx, container)

    def _flush_run(self, run: list, out: list[Block], ctx: dict, parent: Tag) -> None:
        if not run:
            return
        text = "".join(self.inline(n) for n in run)
        bold = _all_bold(run, parent)
        self._emit_para(text, out, ctx, is_h=False, bold=bold, align=_align(parent))

    def _block(self, el: Tag, out: list[Block], ctx: dict) -> None:
        name = el.name
        if name == "table":
            out.extend(self._table(el, ctx))
            return
        if name in ("ul", "ol"):
            out.append(Block("list", self._list(el, ctx)))
            return
        if name == "hr":
            return
        if name == "pre":
            txt = norm_chars(el.get_text())
            if txt.strip():
                out.append(Block("para", "\n".join(_esc_line(l.rstrip()) for l in txt.strip("\n").split("\n"))))
            return
        has_block_child = any(self._is_blockish(c) for c in el.children)
        if has_block_child or name not in BLOCK_TAGS:
            self._walk(el, out, ctx)
            return
        text = "".join(self.inline(c) for c in el.children)
        self._emit_para(text, out, ctx, is_h=name in HEADING_TAGS,
                        bold=_all_bold(list(el.children), el), align=_align(el))

    def _emit_para(self, text: str, out: list[Block], ctx: dict, is_h: bool, bold: bool, align: str) -> None:
        lines = [l.strip() for l in text.split("\n")]
        while lines and not lines[0]:
            lines.pop(0)
        while lines and not lines[-1]:
            lines.pop()
        if not lines:
            return
        flat = " ".join(l for l in lines if l)
        candidate = (is_h or bold) and len(flat) < 600
        appendix_ctx = ctx.get("layout_cell") or align in ("right", "center")
        first_level = _classify(lines[0])
        if first_level == 2 and RE_APPENDIX.match(lines[0]) and not (candidate or appendix_ctx):
            first_level = 0
        if first_level and RE_APPENDIX.match(lines[0]) and len(flat) > 300:
            first_level = 0
        if first_level and (candidate or RE_APPENDIX.match(lines[0])):
            heads: list[list] = []
            for l in lines:
                if not l:
                    continue
                lev = _classify(l)
                if lev and not (RE_APPENDIX.match(l) and heads):
                    heads.append([lev, l])
                else:
                    heads[-1][1] += " " + l
            for lev, t in heads:
                out.append(Block("heading", t, level=lev, is_h=is_h))
            return
        last = next((l for l in reversed(lines) if l), "")
        if (candidate or appendix_ctx) and len(flat) < 300 and RE_APPENDIX_END.search(last):
            out.append(Block("heading", flat, level=2, is_h=is_h))  # «…қаулысына 1-қосымша»
            return
        if is_h and out and out[-1].kind == "heading" and RE_SHORT_LABEL.match(out[-1].text):
            out[-1].text += " " + flat  # "Раздел I" + "Основы конституционного строя"
            return
        if is_h and not ctx.get("in_table") and RE_NUMBERED_SECTION.match(flat) and len(flat) < 300:
            out.append(Block("heading", flat, level=3, is_h=True))
            return
        body = "\n".join(_esc_line(l) for l in _squeeze_blank(lines))
        out.append(Block("para", body, is_h=is_h))

    # -- lists --------------------------------------------------------------
    def _list(self, el: Tag, ctx: dict) -> str:
        ordered = el.name == "ol"
        try:
            n = int(el.get("start") or 1)
        except ValueError:
            n = 1
        items = []
        for li in el.find_all("li", recursive=False):
            sub = self.blocks(li, ctx)
            body = "\n\n".join(render_blocks(sub, top=False)) or ""
            marker = f"{n}. " if ordered else "- "
            pad = " " * len(marker)
            lines = body.split("\n")
            items.append(marker + lines[0] + "".join("\n" + (pad + l if l else "") for l in lines[1:]))
            n += 1
        return "\n".join(items)

    # -- tables -------------------------------------------------------------
    def _table(self, t: Tag, ctx: dict, force_html: bool = False) -> list[Block]:
        rows = [tr for tr in t.find_all("tr") if tr.find_parent("table") is t]
        grid = []
        complex_ = False
        for tr in rows:
            cells = [c for c in tr.find_all(["td", "th"], recursive=False)]
            grid.append(cells)
            for c in cells:
                if _span(c, "rowspan") > 1 or _span(c, "colspan") > 1 or c.find("table"):
                    complex_ = True
        caption = t.find("caption", recursive=False)
        nonempty = [[c for c in r if _cell_has_content(c)] for r in grid]
        if not any(nonempty) and not caption:
            return []
        # Вёрстка (каждая строка — максимум одна непустая ячейка): выводим содержимое как текст.
        if not force_html and not caption and all(len(r) <= 1 for r in nonempty):
            out: list[Block] = []
            for r in nonempty:
                for c in r:
                    out.extend(self.blocks(c, {**ctx, "layout_cell": True}))
            return out
        if force_html or complex_ or caption:
            return [Block("table", self._html_table(t, rows, caption))]
        ncol = max(len(r) for r in grid)
        md_rows = []
        for cells in grid:
            vals = [self._cell_md(c) for c in cells] + [""] * (ncol - len(cells))
            md_rows.append("| " + " | ".join(vals) + " |")
        md_rows.insert(1, "| " + " | ".join(["---"] * ncol) + " |")
        return [Block("table", "\n".join(md_rows))]

    def _cell_md(self, c: Tag) -> str:
        sub = self.blocks(c, {"in_table": True})
        txt = "<br>".join(_block_plainish(b) for b in sub)
        return txt.replace("\n", "<br>").replace("|", "\\|")

    def _html_table(self, t: Tag, rows: list[Tag], caption) -> str:
        parts = ["<table>"]
        if caption is not None:
            parts.append(f"<caption>{self._cell_html(caption)}</caption>")
        for tr in rows:
            cells = []
            for c in tr.find_all(["td", "th"], recursive=False):
                attrs = ""
                for a in ("rowspan", "colspan"):
                    if _span(c, a) > 1:
                        attrs += f' {a}="{_span(c, a)}"'
                cells.append(f"<{c.name}{attrs}>{self._cell_html(c)}</{c.name}>")
            parts.append("<tr>" + "".join(cells) + "</tr>")
        parts.append("</table>")
        return "\n".join(parts)

    def _cell_html(self, c: Tag) -> str:
        pieces = []
        run: list = []

        def flush():
            if run:
                txt = "".join(self.inline(n) for n in run)
                txt = "\n".join(l.strip() for l in txt.split("\n")).strip()
                if txt:
                    pieces.append(html_lib.escape(txt, quote=False).replace("\n", "<br>"))
                run.clear()

        for ch in c.children:
            if isinstance(ch, Tag) and ch.name == "table":
                flush()
                inner_rows = [tr for tr in ch.find_all("tr") if tr.find_parent("table") is ch]
                pieces.append(self._html_table(ch, inner_rows, ch.find("caption", recursive=False)).replace("\n", ""))
            elif isinstance(ch, Tag) and ch.find("table") is not None:
                flush()
                pieces.append(self._cell_html(ch))
            elif isinstance(ch, Tag) and ch.name in BLOCK_TAGS:
                flush()
                run.append(ch)
                flush()
            else:
                run.append(ch)
        flush()
        return "<br>".join(p for p in pieces if p)


def _span(c: Tag, attr: str) -> int:
    try:
        return int(str(c.get(attr) or "1").strip() or 1)
    except ValueError:
        return 1


def _cell_has_content(c: Tag) -> bool:
    return bool(norm_ws(c.get_text())) or c.find("img") is not None


def _block_plainish(b: Block) -> str:
    if b.kind == "heading":
        return b.text
    return b.text


def _align(el: Tag) -> str:
    a = str(el.get("align") or "").lower()
    style = str(el.get("style") or "").lower()
    if "text-align" in style:
        m = re.search(r"text-align\s*:\s*(\w+)", style)
        if m:
            a = m.group(1)
    if el.name == "center":
        a = "center"
    return a


def _all_bold(nodes: list, parent: Tag) -> bool:
    """Весь значимый текст находится внутри <b>/<strong>."""
    found = False
    for n in nodes:
        strings = [n] if isinstance(n, NavigableString) else (n.find_all(string=True) if isinstance(n, Tag) else [])
        for s in strings:
            if isinstance(s, Comment) or not norm_ws(str(s)):
                continue
            found = True
            p = s.parent
            ok = False
            while p is not None and p is not parent.parent:
                if p.name in ("b", "strong"):
                    ok = True
                    break
                if p is parent:
                    break
                p = p.parent
            if not ok:
                return False
    return found


def _classify(line: str) -> int:
    if RE_PART.match(line) or RE_SECTION.match(line):
        return 2
    if RE_APPENDIX.match(line):
        return 2
    if RE_CHAPTER.match(line):
        return 3
    if RE_ARTICLE.match(line):
        return 4
    return 0


def _squeeze_blank(lines: list[str]) -> list[str]:
    out = []
    for l in lines:
        if not l and out and not out[-1]:
            continue
        out.append(l)
    return out


def _esc_line(l: str) -> str:
    """Экранирует символы, которые MarkdownHeaderTextSplitter/Markdown примут за разметку."""
    if l.startswith(("#", "```", "~~~", "|", ">")):
        return "\\" + l
    return l


def render_blocks(blocks: list[Block], top: bool = True) -> list[str]:
    out = []
    for b in blocks:
        if b.kind == "heading":
            if top:
                out.append("#" * b.level + " " + b.text)
            else:
                out.append(b.text)
        else:
            out.append(b.text)
    return out


# ---------------------------------------------------------------------------
# Проверка полноты (независимо от генератора)
# ---------------------------------------------------------------------------
def source_tokens(container: Tag) -> list[str]:
    el = copy.copy(container)
    for t in el.find_all(True):
        if t.name in BLOCK_TAGS or t.name == "br":
            t.insert_before(NavigableString(" "))
            t.insert_after(NavigableString(" "))
    return norm_ws(el.get_text("")).split()


_RE_IMG = re.compile(r"!\[(?:\\.|[^\]\\])*\]\([^)\s]*\)")
_RE_LINK = re.compile(r"\[((?:\\.|[^\]\\])*)\]\([^)\s]*\)")
_RE_SEP_ROW = re.compile(r"^\|(?:\s*:?-{3,}:?\s*\|)+$")
_RE_UNESC = re.compile(r"\\([\\\[\]|#`>~*_-])")


def markdown_to_plain(md: str) -> str:
    out = []
    in_html = False
    for line in md.split("\n"):
        s = line
        if s.startswith("<table>"):
            in_html = True
        if in_html:
            if s.startswith("</table>"):
                in_html = False
            s = html_lib.unescape(re.sub(r"<[^>]+>", " ", re.sub(r"</?su[pb]>", "", s)))
            out.append(_RE_LINK.sub(lambda m: m.group(1), _RE_IMG.sub(" ", s)))
            continue
        if s.startswith("|") and s.rstrip().endswith("|"):
            if _RE_SEP_ROW.match(s.strip()):
                continue
            s = s.replace("<br>", " ")
            s = re.sub(r"(?<!\\)\|", " ", s)
        s = re.sub(r"^#{1,6}\s+", "", s)
        s = _RE_IMG.sub(" ", s)
        s = _RE_LINK.sub(lambda m: m.group(1), s)
        s = re.sub(r"</?su[pb]>", "", s)
        s = _RE_UNESC.sub(r"\1", s)
        out.append(s)
    return "\n".join(out)


def article_labels_source(container: Tag) -> Counter:
    c: Counter = Counter()
    for el in container.find_all(list(BLOCK_TAGS - {"table", "tr", "tbody", "thead", "tfoot", "ul", "ol"})):
        if el.find(list(BLOCK_TAGS)) is not None:
            continue
        m = RE_ARTICLE_LABEL.match(norm_ws(el.get_text()))
        if m:
            c[m.group(0).rstrip(".-–")] += 1
    return c


def article_labels_output(plain: str) -> Counter:
    c: Counter = Counter()
    for line in plain.split("\n"):
        m = RE_ARTICLE_LABEL.match(norm_ws(line))
        if m:
            c[m.group(0).rstrip(".-–")] += 1
    return c


def validate(container: Tag, body_md: str) -> dict:
    src = source_tokens(container)
    plain = markdown_to_plain(body_md)
    out = norm_ws(plain).split()
    sc, oc = Counter(src), Counter(out)
    missing = sc - oc
    extra = oc - sc
    lab_s, lab_o = article_labels_source(container), article_labels_output(plain)
    lab_missing = lab_s - lab_o
    leftovers = [u for u in UI_LEFTOVERS if u in body_md]
    numbers_s = Counter(t for t in src if re.search(r"\d", t))
    numbers_o = Counter(t for t in out if re.search(r"\d", t))
    num_missing = numbers_s - numbers_o
    problems = []
    if not src:
        problems.append("source_container_empty")
    if missing:
        problems.append("tokens_missing")
    if lab_missing:
        problems.append("article_headings_missing")
    if leftovers:
        problems.append("ui_leftovers")
    return {
        "ok": not problems,
        "problems": problems,
        "source_tokens": len(src),
        "output_tokens": len(out),
        "missing_tokens": sum(missing.values()),
        "missing_sample": [t for t, _ in missing.most_common(20)],
        "extra_tokens": sum(extra.values()),
        "extra_sample": [t for t, _ in extra.most_common(20)],
        "numbers_missing": sum(num_missing.values()),
        "article_labels_source": sum(lab_s.values()),
        "article_labels_missing": [k for k in lab_missing][:20],
        "exact_token_sequence": src == out,
        "ui_leftovers": leftovers,
    }


# ---------------------------------------------------------------------------
# Разбор страницы документа целиком
# ---------------------------------------------------------------------------
MONTHS = {"января": 1, "февраля": 2, "марта": 3, "апреля": 4, "мая": 5, "июня": 6, "июля": 7,
          "августа": 8, "сентября": 9, "октября": 10, "ноября": 11, "декабря": 12}
RE_DATE_WORDS = re.compile(r"от\s+(\d{1,2})\s+(" + "|".join(MONTHS) + r")\s+(\d{4})\s*(?:года|г\.?)", re.I)
RE_DATE_DOTS = re.compile(r"от\s+(\d{2})\.(\d{2})\.(\d{4})")
RE_ENTRY = re.compile(r"вводится в действие|вступает в силу|введения в действие|вступления в силу|вводятся в действие|вступают в силу"
                      r"|қолданысқа енгізіл|күшіне ен", re.I)
MONTHS_KK = {"қаңтар": 1, "ақпан": 2, "наурыз": 3, "сәуір": 4, "мамыр": 5, "маусым": 6, "шілде": 7,
             "тамыз": 8, "қыркүйек": 9, "қазан": 10, "қараша": 11, "желтоқсан": 12}
RE_DATE_KK = re.compile(r"(\d{4})\s*жылғы\s+(\d{1,2})\s+(" + "|".join(MONTHS_KK) + r")", re.I)
RE_DATE_KK2 = re.compile(r"(\d{1,2})\s+(" + "|".join(MONTHS_KK) + r")\w*\s+(\d{4})\s*ж", re.I)

STATUS_VERIFIED = {  # (css-класс, текст) -> нормализованный статус; проверено по выдаче st=new|upd = «Действующий»
    ("status_new", "Новый"): "active",
    ("status_upd", "Обновленный"): "active",
    ("status_yts", "Утративший силу"): "not_in_force",
    ("status_new", "Жаңа"): "active",          # kk: выдача st=new|upd = «Мәртебесі ( Қолданылып жүрген )»
    ("status_upd", "Жаңартылған"): "active",
}


def status_info(el: Tag | None) -> dict:
    if el is None:
        return {"raw": None, "css_class": None, "normalized": None}
    classes = [c for c in (el.get("class") or []) if c != "status"]
    cls = classes[0] if classes else None
    raw = norm_ws(el.get_text())
    return {"raw": raw, "css_class": cls, "normalized": STATUS_VERIFIED.get((cls, raw))}


def parse_date_ru(text: str | None) -> str | None:
    if not text:
        return None
    m = RE_DATE_WORDS.search(text)
    if m:
        return f"{int(m.group(3)):04d}-{MONTHS[m.group(2).lower()]:02d}-{int(m.group(1)):02d}"
    m = RE_DATE_KK.search(text)
    if m:
        return f"{int(m.group(1)):04d}-{MONTHS_KK[m.group(3).lower()]:02d}-{int(m.group(2)):02d}"
    m = RE_DATE_KK2.search(text)
    if m:
        return f"{int(m.group(3)):04d}-{MONTHS_KK[m.group(2).lower()]:02d}-{int(m.group(1)):02d}"
    m = RE_DATE_DOTS.search(text)
    if m:
        return f"{m.group(3)}-{m.group(2)}-{m.group(1)}"
    return None


def dotted_to_iso(v: str | None) -> str | None:
    if not v:
        return None
    m = re.fullmatch(r"\s*(\d{2})\.(\d{2})\.(\d{4})\s*", v)
    return f"{m.group(3)}-{m.group(2)}-{m.group(1)}" if m else None


class PageError(Exception):
    def __init__(self, kind: str, detail: str = ""):
        super().__init__(f"{kind}: {detail}")
        self.kind = kind
        self.detail = detail


def check_document_page(soup: BeautifulSoup, html_text: str, doc_id: str) -> None:
    """HTTP 200 ещё не успех: убеждаемся, что это страница нужного документа."""
    has_article = soup.select_one(".container_gamma.text article") is not None
    if not has_article and BLOCKED_RE.search(html_text):
        raise PageError("blocked", "похоже на CAPTCHA/блокировку")
    h1 = soup.select_one(".container_alpha.slogan h1")
    if h1 is None:
        if soup.select_one(".serp") is not None or soup.find("form", action=re.compile("search")) and not has_article:
            raise PageError("not_document", "получена поисковая страница вместо документа")
        raise PageError("not_document", "нет заголовка документа (.container_alpha.slogan h1)")
    tabs = soup.select_one("#tabs_container")
    hrefs = [a.get("href", "") for a in (tabs.find_all("a") if tabs else [])]
    if not any(urlsplit(h).path.rstrip("/").split("/")[3:4] == [doc_id] or h.endswith(f"/docs/{doc_id}") for h in hrefs):
        raise PageError("id_mismatch", f"вкладки страницы не ссылаются на {doc_id}")


def decorative_images(container: Tag, page_url: str) -> dict:
    """Декоративными считаются только однозначные случаи: разделители/распорки (одна сторона ≤ 3 px)
    и точки-маркеры (обе стороны ≤ 12 px). Значки ~20 px неотличимы от символов формул без просмотра
    самого файла (/files/ закрыт robots.txt), поэтому консервативно считаются содержательными."""
    out = {}
    for img in container.find_all("img"):
        src = img.get("src")
        if not src:
            continue
        u = abs_url(src, page_url)
        try:
            w, h = int(str(img.get("width")).strip()), int(str(img.get("height")).strip())
        except (TypeError, ValueError):
            w = h = None
        reason = None
        if w is not None and h is not None:
            if min(w, h) <= 3:
                reason = f"{w}x{h}px: линия/распорка"
            elif max(w, h) <= 12:
                reason = f"{w}x{h}px: маркер"
        prev = out.get(u)
        if prev is not None and not prev["decorative"]:
            continue
        out[u] = {"decorative": reason is not None, "decorative_reason": reason}
    return out


def convert_article(article: Tag, page_url: str) -> dict:
    """Общая часть для страницы старого сайта и text_content API: контейнер текста -> Markdown + проверка."""
    container, warnings = preprocess_container(article)
    conv = Converter(page_url)
    blocks = conv.blocks(container)
    body_md = "\n\n".join(render_blocks(blocks)).strip() + "\n"
    body_md = body_md.replace(TAG_OPEN, "<").replace(TAG_CLOSE, ">")
    check = validate(container, body_md)

    attachments = []
    seen = set()
    deco = decorative_images(container, page_url)
    for m in conv.media:
        if m["url"] not in seen:
            seen.add(m["url"])
            attachments.append({**m, "link_text": None, **deco.get(m["url"], {"decorative": False})})
    for l in conv.links:
        u = l["url"]
        path = urlsplit(u).path
        if u in seen:
            continue
        if "/files/" in path or ATTACH_EXT.search(path) or re.search(r"/download(?:/|$)", path):
            seen.add(u)
            attachments.append({"kind": "file", "url": u, "link_text": l["text"]})

    entry_raw = []
    for el in container.find_all(["p", "span", "font", "div"]):
        if el.find(["p", "div"]) is not None:
            continue
        t = norm_ws(el.get_text())
        if t and RE_ENTRY.search(t) and t not in entry_raw:
            entry_raw.append(t)
        if len(entry_raw) >= 15:
            break
    return {
        "body_md": body_md,
        "validation": check,
        "attachments": attachments,
        "internal_links": len(conv.links),
        "headings": Counter(b.level for b in blocks if b.kind == "heading"),
        "tables": sum(1 for b in blocks if b.kind == "table"),
        "html_tables": sum(1 for b in blocks if b.kind == "table" and b.text.startswith("<table>")),
        "first_heading": next((b.text for b in blocks if b.kind == "heading"), None),
        "entry_into_force_raw": entry_raw,
        "warnings": warnings,
    }


def parse_document(html_text: str, doc_id: str, page_url: str) -> dict:
    soup = BeautifulSoup(html_text, "lxml")
    check_document_page(soup, html_text, doc_id)
    slogan = soup.select_one(".container_alpha.slogan")
    title = norm_ws(slogan.find("h1").get_text())
    requisites = None
    p = slogan.find("p")
    if p is not None:
        requisites = norm_ws(p.get_text()) or None
    page_status = status_info(slogan.select_one(".status"))

    text_div = soup.select_one(".container_gamma.text")
    articles = text_div.find_all("article") if text_div else []
    if not articles:
        raise PageError("no_text_container", "нет .container_gamma.text article")
    res = convert_article(articles[0], page_url)
    if len(articles) != 1:
        res["warnings"].append(f"multiple_article_containers:{len(articles)}")
    res.update({
        "title": title,
        "requisites": requisites,
        "page_status": page_status,
        "text_container_classes": text_div.get("class") if text_div else None,
        "adoption_date_from_requisites": parse_date_ru(requisites),
    })
    return res


# ---------------------------------------------------------------------------
# API нового сайта: https://adilet.zan.kz/api/documents/by-ngr/{id}?language=rus|kaz
# ---------------------------------------------------------------------------
API_BASE = "https://adilet.zan.kz/api/documents/by-ngr/"
API_LANG = {"ru": "rus", "kk": "kaz"}  # из клиентского кода сайта: {ru:"rus", kz:"kaz", en:"eng"}; проверено ответом API
KK_LETTERS = set("әғқңөұүһіӘҒҚҢӨҰҮҺІ")
# Статусы API совпали с CSS-классами страницы старого сайта (K2600000000: ru err/status_err, kk new/status_new).
API_STATUS_VERIFIED = {"new": "active", "upd": "active"}
API_DATE_FIELDS = ("adoption_date", "publication_date", "start_date", "end_date", "justice_reg_date",
                   "created_at", "updated_at", "deleted_at")
# Календарная дата: источник хранит местную полночь UTC+06:00 (K2600000000: adoption_date
# 2026-03-14T18:00:00Z = 15.03.2026 на вкладке «Информация» и в реквизитах). Для каждого документа
# дополнительно сверяется с датой из строки реквизитов; расхождение -> предупреждение.
API_TZ_HOURS = 6


def api_url(doc_id: str, lang: str) -> str:
    from urllib.parse import quote
    return f"{API_BASE}{quote(doc_id)}?language={API_LANG[lang]}"


def api_calendar_date(v: str | None) -> str | None:
    if not v:
        return None
    from datetime import datetime, timedelta
    try:
        dt = datetime.fromisoformat(v.replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        return None
    return (dt + timedelta(hours=API_TZ_HOURS)).date().isoformat()


def parse_api_document(raw: bytes | str, doc_id: str, lang: str, page_url: str) -> dict:
    import json as _json
    text = raw.decode("utf-8", errors="replace") if isinstance(raw, bytes) else raw
    try:
        data = _json.loads(text)
    except ValueError:
        if BLOCKED_RE.search(text[:50000]):
            raise PageError("blocked", "API вернул страницу CAPTCHA/блокировки вместо JSON")
        raise PageError("api_bad_json", "ответ API не JSON")
    d = data.get("document") if isinstance(data, dict) else None
    if not isinstance(d, dict) or not d.get("id"):
        raise PageError("api_no_document", "в ответе нет document.id")
    if d.get("group_number") != doc_id:
        raise PageError("api_id_mismatch", f"запрошен {doc_id}, получен {d.get('group_number')!r}")
    if d.get("language") != API_LANG[lang]:
        raise PageError("api_language_mismatch", f"запрошен язык {API_LANG[lang]}, получен {d.get('language')!r}")
    if d.get("deleted_at"):
        raise PageError("api_deleted", f"deleted_at={d.get('deleted_at')}")
    html_text = d.get("text_content") or ""
    if d.get("is_stub") or d.get("has_content") is False or not html_text.strip():
        raise PageError("api_stub", f"is_stub={d.get('is_stub')}, has_content={d.get('has_content')}, len={len(html_text)}")
    soup = BeautifulSoup(html_text, "lxml")
    root = soup.find("article") or soup.body or soup
    plain = norm_ws(root.get_text())
    if len(plain) < 20:
        raise PageError("api_stub", f"почти пустой текст ({len(plain)} симв.)")
    title = norm_ws(d.get("title_ru") or "")  # проверено: title_ru — название на языке документа
    if not title:
        raise PageError("api_no_title", "пустое название (title_ru)")
    requisites = norm_ws(d.get("official_name") or "") or None  # = строка реквизитов на языке документа

    res = convert_article(root, page_url)
    cyr = sum(1 for c in plain if ("а" <= c.lower() <= "я") or c in "ёЁ" or c in KK_LETTERS)
    kk_ratio = sum(1 for c in plain if c in KK_LETTERS) / max(1, cyr)
    if cyr > 200 and lang == "kk" and kk_ratio < 0.01:
        res["warnings"].append(f"text_language_mismatch:kk_letters={kk_ratio:.3%}")
    if cyr > 200 and lang == "ru" and kk_ratio > 0.03:
        res["warnings"].append(f"text_language_mismatch:kk_letters={kk_ratio:.3%}")

    code = d.get("status") or None
    raw_dates = {k: (d.get(k) or None) for k in API_DATE_FIELDS}
    req_date = parse_date_ru(requisites)
    adopt = api_calendar_date(raw_dates["adoption_date"])
    if adopt and req_date and adopt != req_date:
        res["warnings"].append(f"adoption_date_api_vs_requisites:{adopt}!={req_date}")
    res.update({
        "title": title,
        "requisites": requisites,
        "page_status": {"raw": code, "css_class": f"status_{code}" if code else None,
                        "normalized": API_STATUS_VERIFIED.get(code)},
        "text_container_classes": None,
        "adoption_date_from_requisites": req_date,
        "api": {
            "id": d.get("id"),
            "group_number": d.get("group_number"),
            "language": d.get("language"),
            "status": code,
            "is_actual": d.get("is_actual"),
            "is_actual_note": "не доказательство юридической силы",
            "is_stub": d.get("is_stub"),
            "has_content": d.get("has_content"),
            "content_size": d.get("content_size"),
            "text_content_length": len(html_text),
            "title_ru_field": d.get("title_ru"),
            "title_kz_field": d.get("title_kz"),
            "official_name": d.get("official_name"),
            "document_type": d.get("document_type"),
            "registration_number": d.get("registration_number"),
            "state_registry_number": d.get("state_registry_number"),
            "adoption_number": d.get("adoption_number"),
            "publication_reference": d.get("publication_reference"),
            "dates_raw": raw_dates,
            "dates_calendar": {
                "adoption_date": adopt,
                "publication_date": api_calendar_date(raw_dates["publication_date"]),
                "method": f"UTC + {API_TZ_HOURS} ч (местная полночь источника; сверка с реквизитами по каждому документу)",
            },
            "created_updated_note": "created_at/updated_at — метки записи в базе, не даты юридических изменений",
        },
    })
    return res


def parse_info(html_text: str) -> dict:
    soup = BeautifulSoup(html_text, "lxml")
    t = soup.select_one("table.params")
    if t is None:
        raise PageError("no_info_table", "нет table.params на вкладке «Информация»")
    raw = {}
    for tr in t.find_all("tr"):
        tds = tr.find_all("td")
        if len(tds) == 2:
            raw[norm_ws(tds[0].get_text())] = norm_ws(tds[1].get_text()) or None
    return raw


def sha256(data: bytes | str) -> str:
    if isinstance(data, str):
        data = data.encode("utf-8")
    return hashlib.sha256(data).hexdigest()
