"""HTTP: общий лимит частоты на домен, адаптивная параллельность, повторы, robots.txt."""
from __future__ import annotations

import asyncio
import os
import random
import re
import tempfile
import time
from dataclasses import dataclass, field
from email.utils import parsedate_to_datetime
from urllib import robotparser
from urllib.parse import urlsplit

import json

import aiohttp

from .document import BLOCKED_RE

DEFAULT_UA = "Mozilla/5.0 (compatible; TrafficLawAdiletCollector/1.0)"


class Blocked(Exception):
    """CAPTCHA/явная блокировка — сбор нужно остановить."""


class FetchError(Exception):
    def __init__(self, msg: str, status: int | None = None):
        super().__init__(msg)
        self.status = status


class RobotsDisallowed(Exception):
    pass


@dataclass
class Response:
    url: str
    status: int
    headers: dict
    body: bytes
    elapsed: float

    def text(self) -> str:
        ct = self.headers.get("content-type", "")
        m = re.search(r"charset=([\w\-]+)", ct, re.I)
        enc = m.group(1) if m else "utf-8"
        try:
            return self.body.decode(enc)
        except (LookupError, UnicodeDecodeError):
            return self.body.decode("utf-8", errors="replace")


class RateLimiter:
    """Глобальный лимит запросов/с на домен (каталог, документы, приложения и повторы вместе)."""

    def __init__(self, rps: float):
        self.rps = rps
        self.configured_rps = rps
        self._next = 0.0
        self.pause_until = 0.0
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        while True:
            async with self._lock:
                now = time.monotonic()
                t = max(now, self._next, self.pause_until)
                self._next = t + 1.0 / self.rps
            if t > now:
                await asyncio.sleep(t - now)
            if time.monotonic() >= self.pause_until - 0.01:
                return

    def pause(self, seconds: float) -> None:
        self.pause_until = max(self.pause_until, time.monotonic() + seconds)


class AdaptiveLimit:
    """Семафор с изменяемым пределом."""

    def __init__(self, limit: int):
        self.limit = limit
        self.in_flight = 0
        self.prio_waiting = 0
        self._cond = asyncio.Condition()

    async def acquire(self, priority: bool = False) -> None:
        """priority=True (каталог) получает следующий свободный слот раньше документов."""
        async with self._cond:
            if priority:
                self.prio_waiting += 1
            try:
                await self._cond.wait_for(
                    lambda: self.in_flight < self.limit and (priority or self.prio_waiting == 0))
            finally:
                if priority:
                    self.prio_waiting -= 1
            self.in_flight += 1

    async def release(self) -> None:
        async with self._cond:
            self.in_flight -= 1
            self._cond.notify_all()

    async def set(self, limit: int) -> None:
        async with self._cond:
            self.limit = max(1, limit)
            self._cond.notify_all()


@dataclass
class ReqStat:
    t: float
    ok: bool
    latency: float
    status: int | None
    kind: str
    err: str | None = None  # reset | timeout | http5xx | http429 | http4xx | network


class AiohttpTransport:
    name = "aiohttp"

    def __init__(self, ua: str, max_conn: int, timeout: float):
        self.ua = ua
        self.max_conn = max_conn
        self.timeout = timeout
        self.session: aiohttp.ClientSession | None = None

    async def __aenter__(self):
        conn = aiohttp.TCPConnector(limit=self.max_conn + 2, limit_per_host=self.max_conn + 2,
                                    keepalive_timeout=60, ttl_dns_cache=600)
        self.session = aiohttp.ClientSession(
            connector=conn, cookie_jar=aiohttp.DummyCookieJar(),
            timeout=aiohttp.ClientTimeout(total=self.timeout, connect=30, sock_read=self.timeout),
            headers={"User-Agent": self.ua, "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
                     "Accept-Language": "ru-RU,ru;q=0.9", "Accept-Encoding": "gzip, deflate"},
            auto_decompress=True)
        return self

    async def __aexit__(self, *a):
        if self.session:
            await self.session.close()

    async def request(self, method: str, url: str, headers: dict | None = None, json_body=None) -> Response:
        t0 = time.monotonic()
        async with self.session.request(method, url, allow_redirects=True, headers=headers, json=json_body) as r:
            body = await r.read()
            return Response(str(r.url), r.status, {k.lower(): v for k, v in r.headers.items()}, body,
                            time.monotonic() - t0)


class CurlTransport:
    """Резервный транспорт: curl через subprocess со списком аргументов (без shell)."""
    name = "curl"

    def __init__(self, ua: str, max_conn: int, timeout: float):
        self.ua = ua
        self.timeout = timeout

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        pass

    async def request(self, method: str, url: str, headers: dict | None = None, json_body=None) -> Response:
        t0 = time.monotonic()
        extra = [x for k, v in (headers or {}).items() for x in ("-H", f"{k}: {v}")]
        if json_body is not None:
            extra += ["-X", method, "-H", "Content-Type: application/json",
                      "--data-binary", json.dumps(json_body, ensure_ascii=False)]
        with tempfile.TemporaryDirectory() as td:
            body_p, hdr_p = os.path.join(td, "b"), os.path.join(td, "h")
            proc = await asyncio.create_subprocess_exec(
                "curl", "-sS", "--compressed", "-L", "--max-time", str(int(self.timeout)),
                "-A", self.ua, "-H", "Accept-Language: ru-RU,ru;q=0.9", *extra,
                "-D", hdr_p, "-o", body_p, "-w", "%{http_code} %{url_effective}", url,
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
            out, err = await proc.communicate()
            if proc.returncode != 0:
                raise aiohttp.ClientConnectionError(f"curl exit {proc.returncode}: {err.decode(errors='replace').strip()}")
            code, _, eff = out.decode().partition(" ")
            headers = {}
            with open(hdr_p, encoding="latin-1") as f:
                for line in f:
                    if ":" in line:
                        k, v = line.split(":", 1)
                        headers[k.strip().lower()] = v.strip()
            with open(body_p, "rb") as f:
                body = f.read()
        return Response(eff, int(code), headers, body, time.monotonic() - t0)


class Http:
    def __init__(self, cfg, log):
        self.cfg = cfg
        self.log = log
        transport_cls = CurlTransport if cfg.transport == "curl" else AiohttpTransport
        self.transport = transport_cls(cfg.user_agent, cfg.max_concurrency, cfg.timeout)
        self.rate = RateLimiter(cfg.rps)          # один лимит частоты на все запросы, оба языка и повторы
        self.limit = AdaptiveLimit(cfg.start_concurrency)                    # документы
        self.catalog_limit = AdaptiveLimit(getattr(cfg, "catalog_start_concurrency", 2))  # страницы каталога
        self.stats: list[ReqStat] = []
        self.robots: robotparser.RobotFileParser | None = None
        self.last_429 = 0.0

    async def __aenter__(self):
        await self.transport.__aenter__()
        return self

    async def __aexit__(self, *a):
        await self.transport.__aexit__(*a)

    async def load_robots(self, base: str) -> str:
        r = await self.get(base + "/robots.txt", kind="robots", check_robots=False, max_attempts=4)
        rp = robotparser.RobotFileParser()
        rp.parse(r.text().splitlines())
        self.robots = rp
        return r.text()

    def allowed(self, url: str) -> bool:
        return self.robots is None or self.robots.can_fetch(self.cfg.user_agent, url)

    def _retry_after(self, h: dict) -> float | None:
        v = h.get("retry-after")
        if not v:
            return None
        try:
            return float(v)
        except ValueError:
            try:
                return max(0.0, parsedate_to_datetime(v).timestamp() - time.time())
            except Exception:
                return None

    async def on_429(self, retry_after: float | None) -> None:
        wait = retry_after if retry_after is not None else 60.0
        self.rate.pause(min(wait, 900))
        self.rate.rps = max(0.1, self.rate.rps * 0.5)
        await self.limit.set(max(1, self.limit.limit // 2))
        await self.catalog_limit.set(max(1, self.catalog_limit.limit // 2))
        self.last_429 = time.monotonic()
        self.log(f"HTTP 429: пауза домена {wait:.0f} с, rps -> {self.rate.rps:.2f}, параллельность документов -> "
                 f"{self.limit.limit}, каталога -> {self.catalog_limit.limit}")

    async def get(self, url: str, kind: str = "doc", check_robots: bool = True,
                  max_attempts: int | None = None, headers: dict | None = None) -> Response:
        return await self.fetch("GET", url, kind, check_robots, max_attempts, headers)

    async def post_json(self, url: str, body: dict, kind: str = "search", check_robots: bool = True,
                        max_attempts: int | None = None) -> Response:
        return await self.fetch("POST", url, kind, check_robots, max_attempts,
                                {"Accept": "application/json", "Content-Type": "application/json"}, body)

    async def fetch(self, method: str, url: str, kind: str = "doc", check_robots: bool = True,
                    max_attempts: int | None = None, headers: dict | None = None, json_body=None) -> Response:
        if check_robots and not self.allowed(url):
            raise RobotsDisallowed(url)
        attempts = max_attempts or self.cfg.max_retries
        last_err: Exception | None = None
        for attempt in range(attempts):
            lim = self.catalog_limit if kind in ("catalog", "search") else self.limit
            await lim.acquire()
            t0 = time.monotonic()
            status = None
            err_kind = None
            try:
                await self.rate.acquire()
                t0 = time.monotonic()
                r = await self.transport.request(method, url, headers, json_body)
                status = r.status
            except (aiohttp.ClientError, asyncio.TimeoutError, OSError) as e:
                last_err = e
                r = None
                text = repr(e)
                if isinstance(e, asyncio.TimeoutError) or "Timeout" in type(e).__name__:
                    err_kind = "timeout"
                elif "reset" in text.lower() or "Disconnected" in text or "EOF" in text:
                    err_kind = "reset"
                else:
                    err_kind = "network"
            finally:
                await lim.release()
            lat = time.monotonic() - t0
            if r is not None and r.status == 200:
                self.stats.append(ReqStat(time.time(), True, lat, 200, kind))
                return r
            if r is not None:
                err_kind = "http429" if r.status == 429 else ("http5xx" if r.status >= 500 else "http4xx")
            self.stats.append(ReqStat(time.time(), False, lat, status, kind, err_kind))
            if r is not None:
                if r.status == 429:
                    await self.on_429(self._retry_after(r.headers))
                    last_err = FetchError("HTTP 429", 429)
                    continue
                if r.status in (401, 403, 503) and BLOCKED_RE.search(r.text()[:50000]):
                    raise Blocked(f"HTTP {r.status} с признаками CAPTCHA/блокировки на {url}")
                if r.status == 404:
                    raise FetchError("HTTP 404", 404)
                if 400 <= r.status < 500 and r.status not in (408, 425):
                    raise FetchError(f"HTTP {r.status}: {r.text()[:300]}", r.status)
                last_err = FetchError(f"HTTP {r.status}", r.status)
            delay = min(self.cfg.backoff_cap, self.cfg.backoff_base * (2 ** attempt)) * random.uniform(0.5, 1.5)
            self.log(f"  повтор {attempt + 1}/{attempts} через {delay:.0f} с: {url} ({last_err!r})", verbose=True)
            await asyncio.sleep(delay)
        raise FetchError(f"исчерпаны повторы: {last_err!r}", getattr(last_err, "status", None))


@dataclass
class LevelStat:
    level: int
    started: float
    saves_start: int
    dpm: float = 0.0
    p50: float = 0.0
    err: float = 0.0
    n: int = 0


class Controller:
    """Постепенное повышение параллельности 2 -> 4 -> 8 по полезной скорости и задержкам."""

    def __init__(self, http: Http, cfg, saves_fn, log, limit_attr: str = "limit", kinds: tuple | None = None,
                 name: str = "документов", unit: str = "док/мин", max_level: int | None = None):
        self.http, self.cfg, self.saves_fn, self.log = http, cfg, saves_fn, log
        self.limit_attr, self.kinds, self.name, self.unit = limit_attr, kinds, name, unit
        self.max_level = max_level or cfg.max_concurrency
        start = self.lim.limit
        lv = [2, 4, 8]
        x = 16
        while x <= self.max_level:
            lv.append(x)
            x *= 2
        self.levels = sorted({l for l in lv if l <= self.max_level} | {start, self.max_level})
        self.history: dict[int, LevelStat] = {}
        self.cur = LevelStat(start, time.time(), saves_fn())
        self.ceiling = self.max_level
        self.ceiling_until = 0.0
        self.decisions: list[str] = []

    @property
    def lim(self) -> AdaptiveLimit:
        return getattr(self.http, self.limit_attr)

    def _window(self, since: float):
        s = [x for x in self.http.stats if x.t >= since and (self.kinds is None or x.kind in self.kinds)]
        lat = sorted(x.latency for x in s if x.ok)
        p50 = lat[len(lat) // 2] if lat else 0.0
        # перегрузочные ошибки (таймауты, 5xx, 429) отдельно от быстрых сбросов соединения,
        # которые этот сервер даёт и на одиночных запросах
        err = (sum(1 for x in s if x.err in ("timeout", "http5xx", "http429")) / len(s)) if s else 0.0
        resets = (sum(1 for x in s if x.err in ("reset", "network")) / len(s)) if s else 0.0
        return s, p50, err, resets

    async def _switch(self, new: int, reason: str) -> None:
        old = self.cur.level
        await self.lim.set(new)
        self.cur = LevelStat(new, time.time(), self.saves_fn())
        msg = f"параллельность {self.name} {old} -> {new}: {reason}"
        self.decisions.append(msg)
        self.log(msg)

    async def tick(self) -> None:
        now = time.time()
        if now > self.ceiling_until:
            self.ceiling = self.max_level
        # плавное восстановление частоты после 429
        if self.http.rate.rps < self.http.rate.configured_rps and time.monotonic() - self.http.last_429 > 600:
            self.http.rate.rps = min(self.http.rate.configured_rps, self.http.rate.rps * 1.25)
        if self.lim.limit != self.cur.level:  # уменьшено обработчиком 429
            self.cur = LevelStat(self.lim.limit, now, self.saves_fn())
            self.ceiling, self.ceiling_until = self.cur.level, now + 900
            return
        elapsed = now - self.cur.started
        s, p50, err, resets = self._window(self.cur.started)
        if elapsed < self.cfg.adapt_window or len(s) < 3 * self.cur.level:
            return
        saves = self.saves_fn() - self.cur.saves_start
        self.cur.dpm, self.cur.p50, self.cur.err, self.cur.n = saves / (elapsed / 60), p50, err, len(s)
        self.history[self.cur.level] = self.cur
        lvl = self.cur.level
        info = (f"{self.cur.dpm:.1f} {self.unit}, медиана задержки {p50:.1f} с, таймауты/5xx/429 {err:.0%}, "
                f"сбросы соединения {resets:.0%} ({len(s)} запросов)")
        lower = [l for l in self.levels if l < lvl]
        if (err > 0.10 or resets > 0.35) and lower:
            self.ceiling, self.ceiling_until = lvl, now + 900
            await self._switch(lower[-1], f"доля ошибок высока: {info}")
            return
        prev = self.history.get(lower[-1]) if lower else None
        if prev and prev.dpm > 0 and (self.cur.dpm < prev.dpm * 1.15 or (prev.p50 and p50 > prev.p50 * 1.5)):
            self.ceiling, self.ceiling_until = lvl, now + 900
            await self._switch(prev.level, f"уровень {lvl} не дал прироста ({info}; на {prev.level}: {prev.dpm:.1f} {self.unit}, {prev.p50:.1f} с)")
            return
        cap = self.ceiling if self.ceiling_until > now else self.max_level + 1
        higher = [l for l in self.levels if lvl < l < cap]
        if higher:
            await self._switch(higher[0], f"окно успешно: {info}")
        else:
            self.cur = LevelStat(lvl, now, self.saves_fn())
            self.cur.dpm, self.cur.p50 = self.history[lvl].dpm, self.history[lvl].p50
