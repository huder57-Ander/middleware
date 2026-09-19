import os
import time
import json
import hmac
import hashlib
import logging
from datetime import datetime, timezone
from contextlib import asynccontextmanager
from typing import Any

import httpx
from fastapi import FastAPI, Request, BackgroundTasks, Header
from fastapi.responses import JSONResponse

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("tele2-moysklad-phone")

# ============================================================
# CONFIGURATION
# ============================================================

TELE2_API_URL = os.getenv(
    "TELE2_API_URL",
    "https://ats2.t2.ru/crm/openapi",
).strip().strip("[]'\"").rstrip("/")

TELE2_ACCESS_TOKEN = os.getenv("TELE2_ACCESS_TOKEN", "").strip()
TELE2_REFRESH_TOKEN = os.getenv("TELE2_REFRESH_TOKEN", "").strip()

# MoySklad Phone API (не JSON API)
MOYSKLAD_PHONE_API_URL = os.getenv(
    "MOYSKLAD_PHONE_API_URL",
    "https://api.moysklad.ru/api/phone/1.0",
).strip().rstrip("/")
MOYSKLAD_PHONE_API_KEY = os.getenv("MOYSKLAD_PHONE_API_KEY", "").strip()

# Этот URL нужно указать в поле «Адрес провайдера телефонии» в МойСклад.
PROVIDER_URL = os.getenv(
    "PROVIDER_URL",
    "https://middleware-hudia.onrender.com/api/moysklad/phone",
).strip().rstrip("/")

# Старый внешний endpoint для ручного вызова, если он используется.
CALL_API_KEY = os.getenv("CALL_API_KEY", "").strip()

tele2_client: httpx.AsyncClient | None = None
moysklad_client: httpx.AsyncClient | None = None

# Локальное соответствие externalId -> внутренний id звонка МойСклад.
# Для устойчивости обновление также выполняется по externalId.
call_cache: dict[str, float] = {}
CALL_CACHE_TTL = 86400


# ============================================================
# COMMON HELPERS
# ============================================================

def safe_json(response: httpx.Response) -> Any:
    try:
        return response.json()
    except ValueError:
        return response.text


def normalize_phone(phone: Any) -> str | None:
    if phone is None:
        return None
    digits = "".join(ch for ch in str(phone) if ch.isdigit())
    if digits.startswith("8") and len(digits) == 11:
        digits = "7" + digits[1:]
    if not digits:
        return None
    return digits


def phone_for_moysklad(phone: Any) -> str | None:
    normalized = normalize_phone(phone)
    if not normalized:
        return None
    if normalized.startswith("7") and len(normalized) == 11:
        return "+" + normalized
    return normalized if normalized.startswith("+") else "+" + normalized


def now_moysklad() -> str:
    # Формат, указанный в документации Phone API: yyyy-MM-dd HH:mm:ss.SSS
    return datetime.now(timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


def extract_first(data: Any, *paths: tuple[str, ...]) -> Any:
    if not isinstance(data, dict):
        return None
    for path in paths:
        value: Any = data
        for key in path:
            if not isinstance(value, dict):
                value = None
                break
            value = value.get(key)
        if value not in (None, ""):
            return value
    return None


def get_t2_headers(token: str) -> dict[str, str]:
    clean_token = token.strip().replace("Bearer ", "").strip()

    return {
        "Authorization": clean_token,
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Origin": "https://ats2.t2.ru",
        "Referer": "https://ats2.t2.ru/",
    }



def get_moysklad_phone_headers() -> dict[str, str]:
    return {
        "Lognex-Phone-Auth-Token": MOYSKLAD_PHONE_API_KEY,
        "Accept": "application/json;charset=utf-8",
        "Accept-Encoding": "gzip",
        "Content-Type": "application/json;charset=utf-8",
        "User-Agent": "Tele2-MoySklad-PhoneAPI/1.0",
    }


def remember_call(external_id: str) -> None:
    now = time.time()
    expired = [key for key, value in call_cache.items() if now - value > CALL_CACHE_TTL]
    for key in expired:
        call_cache.pop(key, None)
    call_cache[external_id] = now
    if len(call_cache) > 10000:
        call_cache.pop(next(iter(call_cache)), None)


def md5_upper(value: str) -> str:
    return hashlib.md5(value.encode("utf-8")).hexdigest().upper()


def validate_moysklad_signature(payload: dict[str, Any], signature: str | None) -> bool:
    """Проверка Lognex-Content-MD5.

    В документации указано: MD5 от ключа доступа и значений параметров запроса.
    Допускаем несколько вариантов порядка значений, чтобы не зависеть от порядка
    сериализации полей провайдером.
    """
    if not MOYSKLAD_PHONE_API_KEY:
        logger.warning("MOYSKLAD_PHONE_API_KEY не задан — подпись не проверяется")
        return True
    if not signature:
        return False

    signature = signature.strip().upper()
    values = [str(payload.get(key, "")) for key in ("srcNumber", "destNumber", "uid")]
    candidates = {
        md5_upper(MOYSKLAD_PHONE_API_KEY + "".join(values)),
        md5_upper(MOYSKLAD_PHONE_API_KEY + "".join(sorted(values))),
        md5_upper(MOYSKLAD_PHONE_API_KEY + "".join(str(v) for v in payload.values())),
    }
    return any(hmac.compare_digest(signature, candidate) for candidate in candidates)


# ============================================================
# LIFESPAN
# ============================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    global tele2_client, moysklad_client

    logger.info("🚀 Запуск Tele2 -> МойСклад Phone API Middleware")
    tele2_client = httpx.AsyncClient(
        http2=False,
        follow_redirects=True,
        timeout=httpx.Timeout(connect=20.0, read=90.0, write=30.0, pool=30.0),
    )
    moysklad_client = httpx.AsyncClient(
        timeout=httpx.Timeout(connect=20.0, read=30.0, write=30.0, pool=30.0),
    )

    try:
        yield
    finally:
        logger.info("🛑 Остановка Middleware")
        if tele2_client is not None:
            await tele2_client.aclose()
        if moysklad_client is not None:
            await moysklad_client.aclose()


app = FastAPI(
    title="Tele2 - MoySklad Phone API Middleware",
    version="2.0.0",
    lifespan=lifespan,
)


# ============================================================
# TELE2
# ============================================================

async def refresh_tele2_token() -> bool:
    global TELE2_ACCESS_TOKEN

    if not TELE2_REFRESH_TOKEN or tele2_client is None:
        logger.warning("Нет TELE2_REFRESH_TOKEN или HTTP-клиента")
        return False

    url = f"{TELE2_API_URL}/authorization/refresh/token"
    try:
        response = await tele2_client.put(
            url,
            headers=get_t2_headers(TELE2_REFRESH_TOKEN),
        )
        data = safe_json(response)
        if response.status_code == 200 and isinstance(data, dict) and data.get("accessToken"):
            TELE2_ACCESS_TOKEN = str(data["accessToken"])
            logger.info("🟢 Access Token T2 обновлён")
            return True

        logger.error("Ошибка refresh T2 %s %s", response.status_code, response.text[:300])
    except Exception:
        logger.exception("Ошибка обновления T2 token")
    return False
async def get_t2_employee_full_number(short_number: str):
    """Находит полный номер T2 по внутреннему номеру."""

    if tele2_client is None:
        return None

    short_number = str(short_number or "").strip()

    if not short_number:
        return None

    url = f"{TELE2_API_URL}/employees"

    response = await tele2_client.get(
        url,
        headers=get_t2_headers(TELE2_ACCESS_TOKEN),
    )

    if response.status_code == 401 and await refresh_tele2_token():
        response = await tele2_client.get(
            url,
            headers=get_t2_headers(TELE2_ACCESS_TOKEN),
        )

    response.raise_for_status()

    employees = safe_json(response)

    if isinstance(employees, dict):
        employees = (
            employees.get("employees")
            or employees.get("content")
            or employees.get("data")
            or []
        )

    if not isinstance(employees, list):
        logger.error("Неверный формат ответа T2 /employees")
        return None

    for employee in employees:
        if not isinstance(employee, dict):
            continue

        employee_short = str(
            employee.get("shortNumber") or ""
        ).strip()

        if employee_short == short_number:
            full_number = employee.get("fullNumber")

            if full_number:
                logger.info(
                    "T2 employee mapped: %s -> ***%s",
                    short_number,
                    str(full_number)[-4:],
                )

                return str(full_number).strip()

    logger.error(
        "T2 employee not found by shortNumber: %s",
        short_number,
    )

    return None

async def call_tele2_outgoing(destination: Any, source: Any) -> tuple[bool, Any, int]:
    if tele2_client is None:
        return False, {"message": "HTTP client not ready"}, 503

    clean_destination = normalize_phone(destination)
    clean_source = str(source or "").strip()
    if not clean_destination:
        return False, {"message": "Некорректный номер назначения"}, 400
    if not clean_source:
        return False, {"message": "Не указан внутренний номер source"}, 400

    url = f"{TELE2_API_URL}/call/outgoing"
    params = {
        "destination": clean_destination,
        "source": clean_source,
    }

    try:
        response = await tele2_client.post(
            url,
            headers=get_t2_headers(TELE2_ACCESS_TOKEN),
            params=params,
        )
        if response.status_code in (401, 403) and await refresh_tele2_token():
            response = await tele2_client.post(
                url,
                headers=get_t2_headers(TELE2_ACCESS_TOKEN),
                params=params,
            )

        result = safe_json(response)
        if response.status_code in (200, 201, 202):
            logger.info("📞 Исходящий вызов %s -> %s", clean_source, clean_destination)
            return True, result, response.status_code

        logger.error("T2 outgoing error %s %s", response.status_code, response.text[:300])
        return False, result, response.status_code
    except Exception as exc:
        logger.exception("Ошибка исходящего вызова T2")
        return False, {"message": str(exc)}, 500


# ============================================================
# MOYSKLAD PHONE API CLIENT
# ============================================================

async def moysklad_create_call(
    *,
    external_id: str,
    number: Any,
    extension: Any,
    is_incoming: bool,
    start_time: str | None = None,
    end_time: str | None = None,
    duration: int | None = None,
    event_type: str | None = "SHOW",
) -> tuple[bool, Any]:
    if moysklad_client is None:
        return False, {"message": "MoySklad HTTP client not ready"}
    if not MOYSKLAD_PHONE_API_KEY:
        return False, {"message": "MOYSKLAD_PHONE_API_KEY is not configured"}

    clean_number = phone_for_moysklad(number)
    if not clean_number:
        return False, {"message": "Не удалось определить номер телефона"}

    extension_value = str(extension or "").strip()
    body: dict[str, Any] = {
        "externalId": str(external_id),
        "number": clean_number,
        "isIncoming": bool(is_incoming),
        "startTime": start_time or now_moysklad(),
    }
    if extension_value:
        body["extension"] = extension_value
    if end_time:
        body["endTime"] = end_time
    if duration is not None:
        body["duration"] = duration
    if event_type and extension_value:
        body["events"] = [{
            "eventType": event_type,
            "extension": extension_value,
            "sequence": 1,
        }]

    url = f"{MOYSKLAD_PHONE_API_URL}/call"
    try:
        response = await moysklad_client.post(
            url,
            headers=get_moysklad_phone_headers(),
            json=body,
        )
        result = safe_json(response)
        if response.is_success:
            logger.info("☎️ МойСклад: создан звонок externalId=%s", external_id)
            remember_call(str(external_id))
            return True, result

        logger.error("MoySklad Phone API create error %s %s", response.status_code, response.text[:500])
        return False, {"status_code": response.status_code, "detail": result}
    except Exception as exc:
        logger.exception("Ошибка создания звонка в МойСклад Phone API")
        return False, {"message": str(exc)}


async def moysklad_update_call(
    *,
    external_id: str,
    end_time: str | None = None,
    duration: int | None = None,
    event_type: str | None = "HIDE",
    extension: Any = None,
    record_url: list[str] | None = None,
) -> tuple[bool, Any]:
    if moysklad_client is None:
        return False, {"message": "MoySklad HTTP client not ready"}
    if not MOYSKLAD_PHONE_API_KEY:
        return False, {"message": "MOYSKLAD_PHONE_API_KEY is not configured"}

    body: dict[str, Any] = {}
    if end_time:
        body["endTime"] = end_time
    if duration is not None:
        body["duration"] = duration
    if record_url:
        body["recordUrl"] = record_url

    extension_value = str(extension or "").strip()
    if event_type and (extension_value or event_type in ("HIDE_ALL",)):
        event: dict[str, Any] = {
            "eventType": event_type,
            "sequence": int(time.time() * 1000),
        }
        if extension_value:
            event["extension"] = extension_value
        body["events"] = [event]

    if not body:
        return True, {"status": "nothing_to_update"}

    url = f"{MOYSKLAD_PHONE_API_URL}/call/extid/{external_id}"
    try:
        response = await moysklad_client.put(
            url,
            headers=get_moysklad_phone_headers(),
            json=body,
        )
        result = safe_json(response)
        if response.is_success:
            logger.info("☎️ МойСклад: обновлён звонок externalId=%s", external_id)
            return True, result

        logger.error("MoySklad Phone API update error %s %s", response.status_code, response.text[:500])
        return False, {"status_code": response.status_code, "detail": result}
    except Exception as exc:
        logger.exception("Ошибка обновления звонка в МойСклад Phone API")
        return False, {"message": str(exc)}


# ============================================================
# TELE2 WEBHOOK -> MOYSKLAD PHONE API
# ============================================================

async def process_tele2_event(data: dict[str, Any]) -> None:
    call = data.get("call") if isinstance(data.get("call"), dict) else {}

    external_id = extract_first(
        data,
        ("callId",), ("call_id",), ("id",),
        ("event", "callId"), ("event", "id"),
        ("call", "callId"), ("call", "id"),
    )
    if external_id is None:
        external_id = f"tele2-{int(time.time() * 1000)}"
    external_id = str(external_id)

    caller = extract_first(
        data,
        ("caller",), ("from",), ("phone",), ("number",),
        ("event", "caller"), ("event", "from"),
        ("call", "caller"), ("call", "from"), ("call", "number"),
    )
    callee = extract_first(
        data,
        ("callee",), ("to",), ("destination",),
        ("event", "callee"), ("event", "to"),
        ("call", "callee"), ("call", "to"),
    )
    extension = extract_first(
        data,
        ("extension",), ("user",), ("source",),
        ("event", "extension"), ("call", "extension"),
    )
    event_name = str(extract_first(data, ("eventType",), ("event", "type"), ("type",)) or "").upper()
    is_incoming = bool(extract_first(data, ("isIncoming",), ("incoming",)))
    if not event_name:
        is_incoming = True

    # Если направление не передано явно, для webhook входящего звонка считаем входящим.
    phone = caller or callee
    if not phone:
        logger.warning("Tele2 webhook без номера: %s", data)
        return

    if event_name in {"CALL_END", "END", "HANGUP", "COMPLETED", "CALL_FINISH"}:
        duration = extract_first(data, ("duration",), ("call", "duration"))
        try:
            duration_value = int(duration) if duration is not None else None
        except (TypeError, ValueError):
            duration_value = None
        await moysklad_update_call(
            external_id=external_id,
            end_time=now_moysklad(),
            duration=duration_value,
            event_type="HIDE",
            extension=extension,
        )
        return

    if event_name in {"CALL_ANSWER", "ANSWER", "CONNECTED"}:
        await moysklad_update_call(
            external_id=external_id,
            event_type="STARTTIME",
            extension=extension,
        )
        return

    # Начало входящего вызова: SHOW отображает карточку звонка в МойСклад.
    await moysklad_create_call(
        external_id=external_id,
        number=phone,
        extension=extension,
        is_incoming=is_incoming,
        start_time=now_moysklad(),
        event_type="SHOW",
    )


@app.post("/api/tele2/webhook")
async def tele2_webhook(request: Request, background_tasks: BackgroundTasks):
    try:
        data = await request.json()
        if not isinstance(data, dict):
            return JSONResponse(status_code=400, content={"status": "error", "message": "Ожидался JSON-объект"})
        logger.info("📥 Tele2 webhook: %s", data)
        background_tasks.add_task(process_tele2_event, data)
        return {"status": "ok"}
    except Exception as exc:
        logger.exception("Ошибка Tele2 webhook")
        return JSONResponse(status_code=400, content={"status": "error", "detail": str(exc)})


# ============================================================
# MOYSKLAD PHONE API PROVIDER -> TELE2 OUTGOING CALL
# ============================================================

@app.post("/api/moysklad/phone")
async def moysklad_phone_provider(
    request: Request,
    lognex_content_md5: str | None = Header(
        default=None,
        alias="Lognex-Content-MD5",
    ),
):
    try:
        payload = await request.json()

        logger.info("📥 MoySklad payload: %s", payload)

        if not isinstance(payload, dict):
            return JSONResponse(
                status_code=400,
                content={
                    "status": "error",
                    "message": "Ожидался JSON-объект",
                },
            )

        if not validate_moysklad_signature(
            payload,
            lognex_content_md5,
        ):
            logger.warning("Неверная подпись запроса МойСклад Phone API")

            return JSONResponse(
                status_code=401,
                content={
                    "status": "error",
                    "message": "Invalid signature",
                },
            )

        src_number = (
            payload.get("srcNumber")
            or payload.get("source")
            or payload.get("extension")
        )

        dest_number = (
            payload.get("destNumber")
            or payload.get("destination")
            or payload.get("phone")
        )

        uid = payload.get("uid") or src_number

        # Преобразуем внутренний номер МойСклад
        # в полный номер сотрудника T2
        t2_source_number = await get_t2_employee_full_number(
            src_number
        )

        if not t2_source_number:
            return JSONResponse(
                status_code=404,
                content={
                    "status": "error",
                    "uid": uid,
                    "detail": (
                        f"T2 employee not found: {src_number}"
                    ),
                },
            )

        ok, result, status_code = await call_tele2_outgoing(
            dest_number,
            t2_source_number,
        )

        if ok:
            return {
                "status": "ok",
                "uid": uid,
                "data": result,
            }

        return JSONResponse(
            status_code=(
                502
                if status_code >= 500
                or status_code in (401, 403)
                else status_code
            ),
            content={
                "status": "error",
                "uid": uid,
                "detail": result,
            },
        )

    except Exception as exc:
        logger.exception(
            "Ошибка provider endpoint МойСклад"
        )

        return JSONResponse(
            status_code=400,
            content={
                "status": "error",
                "detail": str(exc),
            },
        )
# ============================================================
# MANUAL OUTGOING CALL ENDPOINT (OPTIONAL)
# ============================================================

@app.post("/api/make-call")
async def make_outgoing_call(
    payload: dict[str, Any],
    x_api_key: str | None = Header(default=None)
):
    if CALL_API_KEY and x_api_key != CALL_API_KEY:
        return JSONResponse(
            status_code=401,
            content={
                "status": "error",
                "message": "Unauthorized"
            }
        )

    phone = payload.get("phone") or payload.get("destination")
    source = payload.get("source") or payload.get("user")

    # Если передан внутренний номер, находим полный номер T2
    t2_source_number = await get_t2_employee_full_number(source)

    if not t2_source_number:
        return JSONResponse(
            status_code=404,
            content={
                "status": "error",
                "detail": f"T2 employee not found: {source}"
            }
        )

    ok, result, status_code = await call_tele2_outgoing(
        phone,
        t2_source_number
    )

    if ok:
        return {
            "status": "success",
            "data": result
        }

    return JSONResponse(
        status_code=status_code if status_code < 500 else 502,
        content={
            "status": "error",
            "detail": result
        }
    )
# ============================================================
# DIAGNOSTICS
# ============================================================

@app.get("/api/test-tele2")
async def test_tele2():
    if tele2_client is None:
        return JSONResponse(status_code=503, content={"status": "error", "message": "HTTP client not ready"})

    url = f"{TELE2_API_URL}/monitoring/calls"
    try:
        response = await tele2_client.get(url, headers=get_t2_headers(TELE2_ACCESS_TOKEN))
        if response.status_code == 401 and await refresh_tele2_token():
            response = await tele2_client.get(url, headers=get_t2_headers(TELE2_ACCESS_TOKEN))
        return {
            "status": "ok" if response.is_success else "error",
            "status_code": response.status_code,
            "body": safe_json(response),
        }
    except Exception as exc:
        logger.exception("Ошибка test-tele2")
        return JSONResponse(status_code=502, content={"status": "error", "detail": str(exc)})


@app.get("/api/test-moysklad-phone")
async def test_moysklad_phone():
    if moysklad_client is None:
        return JSONResponse(status_code=503, content={"status": "error", "message": "HTTP client not ready"})
    if not MOYSKLAD_PHONE_API_KEY:
        return JSONResponse(status_code=500, content={"status": "error", "message": "MOYSKLAD_PHONE_API_KEY is not configured"})

    url = f"{MOYSKLAD_PHONE_API_URL}/employee"
    try:
        response = await moysklad_client.get(
            url,
            headers=get_moysklad_phone_headers(),
            params={"filter": "extention~=0"},
        )
        return {
            "status": "ok" if response.is_success else "error",
            "status_code": response.status_code,
            "body": safe_json(response),
        }
    except Exception as exc:
        logger.exception("Ошибка test-moysklad-phone")
        return JSONResponse(status_code=502, content={"status": "error", "detail": str(exc)})


@app.get("/api/config")
async def config_status():
    return {
        "status": "ok",
        "tele2_api_url": TELE2_API_URL,
        "moysklad_phone_api_url": MOYSKLAD_PHONE_API_URL,
        "provider_url": PROVIDER_URL,
        "tele2_access_token_configured": bool(TELE2_ACCESS_TOKEN),
        "tele2_refresh_token_configured": bool(TELE2_REFRESH_TOKEN),
        "moysklad_phone_api_key_configured": bool(MOYSKLAD_PHONE_API_KEY),
        "call_api_key_configured": bool(CALL_API_KEY),
    }


@app.get("/")
@app.head("/")
async def root():
    return {
        "status": "running",
        "service": "Tele2-MoySklad-PhoneAPI",
        "mode": "phone-api",
        "provider_url": PROVIDER_URL,
    }


@app.get("/health")
async def health():
    return {"status": "ok"}
