"""Посредник: T2 ВАТС (ats2.tele2.ru/crm/openapi) <-> МойСклад Phone API 1.0.

Что делает:
  * МойСклад -> T2: кнопка «Позвонить» (POST /moysklad/callRequest -> T2 /call/outgoing);
  * T2 -> МойСклад: у T2 нет вебхуков, поэтому опрашиваем /monitoring/calls,
    создаём звонок и показываем/скрываем карточку (SHOW / HIDE);
  * история и записи: опрашиваем /call-records/info, дописываем recordUrl
    (ссылка идёт через наш прокси /record/..., т.к. T2 отдаёт файл только с токеном).

Запускать в ОДНОМ процессе (опрос живёт внутри приложения):
  uvicorn main:app --host 0.0.0.0 --port $PORT
"""
import asyncio
import hashlib
import hmac
import json
import logging
import os
import re
from collections import deque
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import quote
from zoneinfo import ZoneInfo

import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import StreamingResponse
from starlette.background import BackgroundTask

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logging.getLogger("httpx").setLevel(logging.WARNING)
log = logging.getLogger("bridge")

# ---------------------------------------------------------------- настройки
T2_BASE = os.getenv("T2_BASE", "https://ats2.t2.ru/crm/openapi")  # ats2.tele2.ru отдаёт 308 на t2.ru
MS_BASE = "https://api.moysklad.ru/api/phone/1.0"
MS_KEY = os.environ["MS_PHONE_KEY"]  # ключ из приложения Phone API в МоёмСкладе
PUBLIC_URL = os.getenv("PUBLIC_URL", "https://moysklad-t2-andreihuder.amvera.io").rstrip("/")
RECORD_SECRET = os.getenv("RECORD_SECRET", MS_KEY)  # для подписи ссылок на записи
TOKEN_FILE = Path(os.getenv("TOKEN_STORE_PATH", "t2_tokens.json"))
PROCESSED_FILE = TOKEN_FILE.with_name("processed_records.json")
POLL_SEC = float(os.getenv("POLL_INTERVAL", "2"))  # опрос текущих звонков
RECORDS_SEC = int(os.getenv("RECORDS_INTERVAL", "60"))  # опрос истории/записей
RECORDS_LOOKBACK_MIN = int(os.getenv("RECORDS_LOOKBACK_MIN", "10"))
TZ = ZoneInfo(os.getenv("TZ_NAME", "Europe/Moscow"))
CLICK_SOURCE = os.getenv("CLICK_SOURCE_MODE", "full")  # full | short: что слать в T2 как source
SIGNATURE_MODE = os.getenv("SIGNATURE_MODE", "enforce")  # enforce | log (только для отладки)

# T2 (WAF) может отклонять запросы с «библиотечным» User-Agent/Accept -> 406, поэтому задаём явно
UA = os.getenv("HTTP_USER_AGENT", "Mozilla/5.0 (compatible; crm-bridge/1.0)")
http = httpx.AsyncClient(timeout=15, headers={"User-Agent": UA})
MS_HEADERS = {"Lognex-Phone-Auth-Token": MS_KEY, "Accept": "application/json;charset=utf-8"}


def now() -> datetime:
    return datetime.now(TZ)


def ms_time(dt: datetime) -> str:
    return dt.astimezone(TZ).strftime("%Y-%m-%d %H:%M:%S.") + f"{dt.microsecond // 1000:03d}"


def norm(num) -> str | None:
    """Цифры; 8XXXXXXXXXX и 10 цифр приводим к 7XXXXXXXXXX. Короткие номера остаются как есть."""
    if not num:
        return None
    d = re.sub(r"\D", "", str(num))
    if len(d) == 11 and d[0] in "78":
        return "7" + d[1:]
    if len(d) == 10:
        return "7" + d
    return d or None


def plus(num) -> str | None:
    n = norm(num)
    return f"+{n}" if n and len(n) >= 10 else n


# ---------------------------------------------------------------- токены T2
class Tokens:
    """access живёт сутки, refresh 7 суток. Новую пару обязательно сохраняем на диск."""

    def __init__(self):
        self.access = os.getenv("T2_ACCESS_TOKEN", "")
        self.refresh = os.getenv("T2_REFRESH_TOKEN", "")
        self.seed = self.refresh  # с какого токена из env начинали
        self.lock = asyncio.Lock()
        if TOKEN_FILE.exists():
            try:
                d = json.loads(TOKEN_FILE.read_text())
                # файл используем, только если в env не подложили новые токены из кабинета АТС
                if d.get("seed") == self.seed:
                    self.access, self.refresh = d["access"], d["refresh"]
            except Exception:
                log.exception("не удалось прочитать %s", TOKEN_FILE)

    def save(self):
        try:
            TOKEN_FILE.parent.mkdir(parents=True, exist_ok=True)
            TOKEN_FILE.write_text(json.dumps(
                {"access": self.access, "refresh": self.refresh, "seed": self.seed}))
        except Exception:
            log.exception("не удалось сохранить токены (нужен постоянный диск!)")

    async def refresh_now(self, stale: str | None = None):
        async with self.lock:
            if stale is not None and self.access != stale:
                return  # уже обновили параллельным запросом
            r = await http.put(f"{T2_BASE}/authorization/refresh/token",
                               headers={"Authorization": self.refresh})
            r.raise_for_status()
            d = r.json()
            self.access = d["accessToken"]
            self.refresh = d.get("refreshToken") or self.refresh
            self.save()
            log.info("токены T2 обновлены")


tokens = Tokens()


async def t2(method: str, path: str, expect: tuple = (), **kw) -> httpx.Response:
    """expect - коды ответа, которые вызывающий код обрабатывает сам (в лог как ошибка не пишем)."""
    for attempt in (1, 2):
        token = tokens.access
        r = await http.request(method, T2_BASE + path,
                               headers={"Authorization": token, "Accept": "application/json"}, **kw)
        if r.status_code in (401, 403) and attempt == 1:
            await tokens.refresh_now(stale=token)
            continue
        if r.status_code >= 400 and r.status_code not in expect:
            log.error("T2 %s %s -> %s, ответ: %s | заголовки: %s", method, path,
                      r.status_code, r.text[:300], dict(r.headers))
        r.raise_for_status()
        return r
    raise RuntimeError("unreachable")


# ---------------------------------------------------------------- сотрудники
emp_by_num: dict[str, str] = {}  # любой номер сотрудника (короткий/полный) -> добавочный
emp_full: dict[str, str] = {}  # добавочный -> полный номер


async def load_employees():
    data = (await t2("GET", "/employees")).json()
    if isinstance(data, dict):
        data = [data]
    by, full = {}, {}
    for e in data:
        short = str(e.get("shortNumber") or "").strip()
        fn = norm(e.get("fullNumber"))
        ext = short or fn
        if not ext:
            continue
        if short:
            by[short] = ext
        if fn:
            by[fn] = ext
            full[ext] = fn
    emp_by_num.clear(); emp_by_num.update(by)
    emp_full.clear(); emp_full.update(full)
    log.info("сотрудников T2: %d, добавочные: %s", len(full), sorted(full))


def resolve(caller, called):
    """-> (добавочный, внешний номер, входящий?) или None (внутренний / ещё не маршрутизирован)."""
    a = emp_by_num.get(norm(caller) or "")
    b = emp_by_num.get(norm(called) or "")
    if a and b:
        return None
    if a:
        return a, called, False
    if b:
        return b, caller, True
    return None


# ---------------------------------------------------------------- МойСклад
async def ms(method: str, path: str, body=None):
    try:
        r = await http.request(method, MS_BASE + path, json=body, headers=MS_HEADERS)
        if r.status_code >= 400:
            log.error("МойСклад %s %s -> %s %s", method, path, r.status_code, r.text[:300])
        return r
    except httpx.HTTPError:
        log.exception("МойСклад недоступен: %s %s", method, path)


@dataclass
class Call:
    ext_id: str
    ext: str
    number: str
    incoming: bool
    start: datetime
    seq: int = 1
    missing: int = 0
    end: datetime | None = None
    has_record: bool = False


live: dict[tuple, Call] = {}
done: deque[Call] = deque(maxlen=500)


async def ms_create(c: Call):
    await ms("POST", "/call", {
        "externalId": c.ext_id, "number": plus(c.number), "extension": c.ext,
        "isIncoming": c.incoming, "startTime": ms_time(c.start),
        "events": [{"eventType": "SHOW", "extension": c.ext, "sequence": 1}],
    })


async def ms_finish(c: Call):
    c.end, c.seq = now(), c.seq + 1
    await ms("PUT", f"/call/extid/{c.ext_id}", {
        "endTime": ms_time(c.end),
        "events": [{"eventType": "HIDE", "extension": c.ext, "sequence": c.seq}],
    })
    done.append(c)


# ---------------------------------------------------------------- опрос текущих звонков
async def poll_calls():
    while True:
        try:
            calls = (await t2("GET", "/monitoring/calls")).json() or []
            seen = set()
            for c in calls:
                caller = c.get("callerNumberFull") or c.get("callerNumberShort")
                called = c.get("calledNumberFull") or c.get("calledNumberShort")
                r = resolve(caller, called)
                if not r:
                    continue
                ext, number, incoming = r
                key = (norm(caller), norm(called))
                seen.add(key)
                if key in live:
                    live[key].missing = 0
                    continue
                start = now()
                call = Call(f"t2-{int(start.timestamp())}-{key[0]}-{key[1]}",
                            ext, number, incoming, start)
                live[key] = call
                log.info("новый звонок %s ext=%s number=%s incoming=%s", call.ext_id, ext, number, incoming)
                await ms_create(call)
            for key, call in list(live.items()):
                if key in seen:
                    continue
                call.missing += 1  # 2 опроса подряд без звонка = завершён (защита от «мигания»)
                if call.missing >= 2:
                    del live[key]
                    log.info("звонок завершён %s", call.ext_id)
                    await ms_finish(call)
        except Exception:
            log.exception("ошибка опроса /monitoring/calls")
            await asyncio.sleep(5)
        await asyncio.sleep(POLL_SEC)


# ---------------------------------------------------------------- история и записи
processed: deque[str] = deque(maxlen=1000)
if PROCESSED_FILE.exists():
    try:
        processed.extend(json.loads(PROCESSED_FILE.read_text()))
    except Exception:
        log.exception("не удалось прочитать %s", PROCESSED_FILE)


def save_processed():
    try:
        PROCESSED_FILE.parent.mkdir(parents=True, exist_ok=True)
        PROCESSED_FILE.write_text(json.dumps(list(processed)))
    except Exception:
        log.exception("не удалось сохранить список обработанных записей")


def sign(name: str) -> str:
    return hmac.new(RECORD_SECRET.encode(), name.encode(), hashlib.sha256).hexdigest()[:32]


def record_url(name: str) -> str:
    # recordFileName у T2 содержит "/" (например "2022-08-12/mo_xxx") - оставляем
    # его как обычный разделитель пути, а не %2F, иначе часть прокси/ASGI-цепочек
    # декодирует %2F ещё до роутинга, и /record/{name} перестаёт совпадать (404)
    return f"{PUBLIC_URL}/record/{quote(name, safe='/')}?sig={sign(name)}"


def parse_ts(v) -> datetime:
    if v is None:
        return now()
    if isinstance(v, (int, float)):
        return datetime.fromtimestamp(v / 1000 if v > 1e12 else v, TZ)
    return datetime.fromisoformat(str(v).replace("Z", "+00:00")).astimezone(TZ)


async def handle_record(row: dict):
    # ВАЖНО: у T2 поле называется "recordFileName", а не "recordName"
    # (см. документацию GET /call-records/info). Из-за неверного имени
    # поля эта функция раньше выходила на первой строке для КАЖДОЙ
    # записи, ещё до любого логирования, и recordUrl никогда не
    # долетал до МоегоСклада.
    name = row.get("recordFileName") or row.get("recordName")
    if not name or name in processed:
        return

    # 1. Более гибкий извлекатель номеров из ответа T2
    caller = (
        row.get("callerNumber") 
        or row.get("srcNumber") 
        or (row.get("callerPart") or {}).get("fullNumber")
        or (row.get("callerPart") or {}).get("shortNumber")
    )
    callee = (
        row.get("calleeNumber") 
        or row.get("dstNumber") 
        or (row.get("calleePart") or {}).get("fullNumber")
        or (row.get("calleePart") or {}).get("shortNumber")
    )

    r = resolve(caller, callee)
    if not r:
        log.warning("запись %s: не нашли сотрудника среди caller=%s / callee=%s", name, caller, callee)
        return

    ext, number, incoming = r
    ts = parse_ts(row.get("callTimestamp") or row.get("callDate") or row.get("startTime"))
    dur = float(row.get("conversationDuration") or row.get("callDuration") or row.get("duration") or 0)
    url = record_url(name)

    best = None
    target_norm_num = norm(number)

    # 2. Ослабляем условия поиска звонка в истории done
    for d in done:
        if d.has_record or d.ext != ext:
            continue
        # Проверяем совпадение номеров
        if target_norm_num and norm(d.number) != target_norm_num:
            continue
            
        # Увеличиваем окно поиска до 30 минут (1800 сек)
        diff = abs((d.start - ts).total_seconds())
        if diff < 1800 and (best is None or diff < best[0]):
            best = (diff, d)

    if best:
        call_obj = best[1]
        call_obj.has_record = True

        # МойСклад ждёт recordUrl как МАССИВ строк, а не строку
        # (см. доку Phone API 1.0: "recordUrl": ["http://..."])
        res = await ms("PUT", f"/call/extid/{call_obj.ext_id}", {
            "recordUrl": [url],
        })
        log.info("запись %s прикреплена к звонку %s (status=%s)", name, call_obj.ext_id, getattr(res, 'status_code', None))
    else:
        # Если не нашли активный звонок, создаем его целиком по данным записи
        log.info("запись %s: звонок не найден в памяти, создаю по записи", name)
        await ms("POST", "/call", {
            "externalId": f"t2-rec-{name}", 
            "number": plus(number), 
            "extension": ext,
            "isIncoming": incoming, 
            "startTime": ms_time(ts),
            "endTime": ms_time(ts + timedelta(seconds=dur)), 
            "recordUrl": [url],  # тоже массив
        })

    processed.append(name)
    save_processed()

async def poll_records():
    last = now() - timedelta(minutes=RECORDS_LOOKBACK_MIN)
    while True:
        await asyncio.sleep(RECORDS_SEC)
        try:
            end = now()
            rows = (await t2("GET", "/call-records/info", params={
                "start": (last - timedelta(minutes=2)).isoformat(timespec="seconds"),
                "end": end.isoformat(timespec="seconds"),
            })).json() or []
            log.info("call-records/info: получено строк %d (окно %s .. %s)",
                      len(rows), (last - timedelta(minutes=2)).isoformat(timespec="seconds"),
                      end.isoformat(timespec="seconds"))
            for row in rows:
                await handle_record(row)
            last = end
        except Exception:
            log.exception("ошибка опроса /call-records/info")


# ---------------------------------------------------------------- фоновые задачи
async def token_loop():
    while True:
        await asyncio.sleep(12 * 3600)
        try:
            await tokens.refresh_now()
        except Exception:
            log.exception("не удалось обновить токены T2 - сгенерируйте новые в кабинете АТС")


async def employees_loop():
    while True:
        try:
            await load_employees()
        except Exception:
            log.exception("не удалось загрузить сотрудников")
            await asyncio.sleep(15)
            continue
        await asyncio.sleep(600)


@asynccontextmanager
async def lifespan(app: FastAPI):
    log.info("ссылки на записи будут вести на %s/record/<имя записи>?sig=...", PUBLIC_URL)
    if RECORD_SECRET == MS_KEY:
        log.warning("RECORD_SECRET не задан: ссылки на записи подписаны ключом Phone API "
                    "и перестанут открываться при его смене")
    tasks = [asyncio.create_task(f()) for f in (token_loop, employees_loop, poll_calls, poll_records)]
    yield
    for t in tasks:
        t.cancel()
    await http.aclose()


app = FastAPI(lifespan=lifespan)


@app.api_route("/health", methods=["GET", "HEAD"])
async def health():
    return {"ok": True, "live_calls": len(live), "employees": len(emp_full)}


# ---------------------------------------------------------------- МойСклад -> T2: «Позвонить»
def signature_ok(raw: bytes, body: dict, header: str | None) -> bool:
    """MD5 от (ключ + значения параметров). Точный порядок в доке не уточнён - пробуем два варианта."""
    if not header:
        return False
    got = header.strip().upper()
    variants = [MS_KEY + "".join(str(v) for v in body.values()), MS_KEY + raw.decode("utf-8", "ignore")]
    return any(hashlib.md5(v.encode()).hexdigest().upper() == got for v in variants)


@app.post("/moysklad/callRequest")
async def call_request(request: Request):
    raw = await request.body()
    try:
        body = json.loads(raw)
        src, dst = str(body["srcNumber"]), norm(body["destNumber"])
    except Exception:
        raise HTTPException(400, "bad body")
    if not signature_ok(raw, body, request.headers.get("Lognex-Content-MD5")):
        log.warning("подпись Lognex-Content-MD5 не совпала (header=%s, поля=%s)",
                    request.headers.get("Lognex-Content-MD5"), list(body))
        if SIGNATURE_MODE == "enforce":
            raise HTTPException(403, "bad signature")
    # T2 не уточняет, какой номер сотрудника ждёт source: пробуем полный и короткий (порядок задаёт CLICK_SOURCE_MODE)
    full = emp_full.get(src)
    order = [full, src] if CLICK_SOURCE == "full" else [src, full]
    candidates = [c for i, c in enumerate(order) if c and c not in order[:i]]
    for source in candidates:
        try:
            await t2("POST", "/call/outgoing", expect=(404,),
                     params={"source": source, "destination": dst})
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:  # сотрудник не найден - пробуем другой формат
                log.warning("click2call: source=%r не подошёл, пробуем следующий формат", source)
                continue
            log.error("T2 click2call: %s %s", e.response.status_code, e.response.text[:300])
            raise HTTPException(502, "T2 error")
        log.info("click2call: source=%r -> %s", source, dst)
        return {"status": "ok"}
    log.error("click2call: T2 не нашёл сотрудника ни по одному из форматов %s (srcNumber из МоегоСклада=%r)",
              candidates, src)
    raise HTTPException(502, "T2: employee not found")


# ---------------------------------------------------------------- прокси записей
async def open_record(name: str) -> httpx.Response:
    for attempt in (1, 2):
        token = tokens.access
        req = http.build_request("GET", f"{T2_BASE}/call-records/file/{quote(name, safe='/')}",
                                 headers={"Authorization": token, "Accept": "*/*"})
        r = await http.send(req, stream=True)
        if r.status_code in (401, 403) and attempt == 1:
            await r.aclose()
            await tokens.refresh_now(stale=token)
            continue
        return r
    raise RuntimeError("unreachable")


@app.get("/record/{name:path}")
async def record(name: str, sig: str):
    log.info("запрос записи name=%r sig=%r", name, sig)
    if not hmac.compare_digest(sig, sign(name)):
        log.warning("запись %s: неверная подпись (ожидалась %s)", name, sign(name))
        raise HTTPException(403, "bad signature")
    r = await open_record(name)
    if r.status_code != 200:
        body = (await r.aread())[:300]
        await r.aclose()
        log.error("T2 отдал %s на файл записи %s: %s", r.status_code, name, body)
        raise HTTPException(404 if r.status_code == 404 else 502,
                             f"T2 status={r.status_code}")
    return StreamingResponse(r.aiter_bytes(),
                             media_type=r.headers.get("content-type", "audio/mpeg"),
                             background=BackgroundTask(r.aclose))
