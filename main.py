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

MOYSKLAD_PHONE_API_URL = os.getenv(
    "MOYSKLAD_PHONE_API_URL",
    "https://api.moysklad.ru/api/phone/1.0",
).strip().rstrip("/")
MOYSKLAD_PHONE_API_KEY = os.getenv("MOYSKLAD_PHONE_API_KEY", "").strip()

PROVIDER_URL = os.getenv(
    "PROVIDER_URL",
    "https://middleware-hudia.onrender.com/api/moysklad/phone",
).strip().rstrip("/")

CALL_API_KEY = os.getenv("CALL_API_KEY", "").strip()

tele2_client: httpx.AsyncClient | None = None
moysklad_client: httpx.AsyncClient | None = None

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
    return datetime.now(timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


def get_t2_headers(token: str) -> dict[str, str]:
    clean_token = token.strip().replace("Bearer ", "").strip("[]'\"")

    return {
        "Authorization": clean_token,
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
        "Origin": "https://ats2.t2.ru",
        "Referer": "https://ats2.t2.ru/",
        "Sec-Ch-Ua": '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": '"Windows"',
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
    }


def get_moysklad_phone_headers() -> dict[str, str]:
    return {
        "Lognex-Phone-Auth-Token": MOYSKLAD_PHONE_API_KEY,
        "Accept": "application/json;charset=utf-8",
        "Accept-Encoding": "gzip",
        "Content-Type": "application/json;charset=utf-8",
        "User-Agent": "Tele2-MoySklad-PhoneAPI/1.0",
    }


def validate_moysklad_signature(payload: dict[str, Any], signature: str | None) -> bool:
    if not MOYSKLAD_PHONE_API_KEY:
        logger.warning("MOYSKLAD_PHONE_API_KEY не задан — подпись не проверяется")
        return True
    if not signature:
        return False

    signature = signature.strip().upper()
    values = [str(payload.get(key, "")) for key in ("srcNumber", "destNumber", "uid")]
    
    def md5_u(v: str) -> str:
        return hashlib.md5(v.encode("utf-8")).hexdigest().upper()

    candidates = {
        md5_u(MOYSKLAD_PHONE_API_KEY + "".join(values)),
        md5_u(MOYSKLAD_PHONE_API_KEY + "".join(sorted(values))),
        md5_u(MOYSKLAD_PHONE_API_KEY + "".join(str(v) for v in payload.values())),
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
# TELE2 SERVICES
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


async def get_t2_employee_full_number(short_number: str) -> str:
    """Находит полный номер T2 по короткому/внутреннему номеру.

    Если Tele2 возвращает ошибку на /employees, возвращается исходный short_number,
    чтобы не блокировать выполнение исходящего вызова.
    """
    short_number = str(short_number or "").strip()
    if not short_number or tele2_client is None:
        return short_number

    url = f"{TELE2_API_URL}/employees"

    try:
        response = await tele2_client.get(
            url,
            headers=get_t2_headers(TELE2_ACCESS_TOKEN),
        )

        if response.status_code in (401, 403) and await refresh_tele2_token():
            response = await tele2_client.get(
                url,
                headers=get_t2_headers(TELE2_ACCESS_TOKEN),
            )

        if not response.is_success:
            logger.warning(
                "T2 /employees недоступен [%s]. Используем исходный номер: %s | Ответ: %s",
                response.status_code,
                short_number,
                response.text[:200]
            )
            return short_number

        employees = safe_json(response)
        if isinstance(employees, dict):
            employees = (
                employees.get("employees")
                or employees.get("content")
                or employees.get("data")
                or []
            )

        if isinstance(employees, list):
            for employee in employees:
                if not isinstance(employee, dict):
                    continue
                employee_short = str(employee.get("shortNumber") or "").strip()
                if employee_short == short_number:
                    full_number = employee.get("fullNumber")
                    if full_number:
                        logger.info("T2 employee mapped: %s -> %s", short_number, full_number)
                        return str(full_number).strip()

    except Exception as exc:
        logger.error("Ошибка сопоставления сотрудника T2: %s", exc)

    return short_number


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

    # Передаём параметры через JSON body, а НЕ params!
    body = {
        "destination": clean_destination,
        "source": clean_source,
    }

    try:
        response = await tele2_client.post(
            url,
            headers=get_t2_headers(TELE2_ACCESS_TOKEN),
            json=body,  # <-- ИСПРАВЛЕНО (было params=params)
        )
        if response.status_code in (401, 403) and await refresh_tele2_token():
            response = await tele2_client.post(
                url,
                headers=get_t2_headers(TELE2_ACCESS_TOKEN),
                json=body,
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
# MOYSKLAD PROVIDER ENDPOINT
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
                content={"status": "error", "message": "Ожидался JSON-объект"},
            )

        if not validate_moysklad_signature(payload, lognex_content_md5):
            logger.warning("Неверная подпись запроса МойСклад Phone API")
            return JSONResponse(
                status_code=401,
                content={"status": "error", "message": "Invalid signature"},
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

        # Безопасно получаем полный номер или используем исходный короткий
        t2_source_number = await get_t2_employee_full_number(src_number)

        ok, result, status_code = await call_tele2_outgoing(
            dest_number,
            t2_source_number,
        )

        if ok:
            return {"status": "ok", "uid": uid, "data": result}

        return JSONResponse(
            status_code=502 if status_code >= 500 or status_code in (401, 403) else status_code,
            content={"status": "error", "uid": uid, "detail": result},
        )

    except Exception as exc:
        logger.exception("Ошибка provider endpoint МойСклад")
        return JSONResponse(
            status_code=400,
            content={"status": "error", "detail": str(exc)},
        )


# ============================================================
# HEALTH & DIAGNOSTICS
# ============================================================

@app.get("/")
@app.head("/")
async def root():
    return {
        "status": "running",
        "service": "Tele2-MoySklad-PhoneAPI",
        "provider_url": PROVIDER_URL,
    }

@app.get("/health")
async def health():
    return {"status": "ok"}
