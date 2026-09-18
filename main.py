import os
import time
import logging
from contextlib import asynccontextmanager
from typing import Any

import httpx
from fastapi import FastAPI, Request, BackgroundTasks, Header
from fastapi.responses import JSONResponse

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("tele2-moysklad")

TELE2_API_URL = os.getenv(
    "TELE2_API_URL",
    "https://ats2.t2.ru/crm/openapi",
).strip().strip("[]'\"").rstrip("/")
TELE2_ACCESS_TOKEN = os.getenv("TELE2_ACCESS_TOKEN", "").strip()
TELE2_REFRESH_TOKEN = os.getenv("TELE2_REFRESH_TOKEN", "").strip()

MOYSKLAD_API_URL = "https://api.moysklad.ru/api/remap/1.2"
MOYSKLAD_TOKEN = os.getenv("MOYSKLAD_TOKEN", "").strip()
WEBHOOK_URL = os.getenv(
    "WEBHOOK_URL",
    "https://middleware-hudia.onrender.com/api/tele2/webhook",
).strip()
CALL_API_KEY = os.getenv("CALL_API_KEY", "").strip()

tele2_client: httpx.AsyncClient | None = None
moysklad_client: httpx.AsyncClient | None = None

processed_calls: dict[str, float] = {}
CALL_CACHE_TTL = 86400


def is_processed(call_id: str) -> bool:
    now = time.time()
    expired = [
        key for key, value in processed_calls.items()
        if now - value > CALL_CACHE_TTL
    ]
    for key in expired:
        processed_calls.pop(key, None)

    if call_id in processed_calls:
        return True

    processed_calls[call_id] = now
    if len(processed_calls) > 10000:
        oldest_key = next(iter(processed_calls))
        processed_calls.pop(oldest_key, None)
    return False


def get_t2_headers(token: str) -> dict[str, str]:
    clean_token = token.replace("Bearer ", "").strip()
    return {
        "Authorization": clean_token,
        "Accept": "application/json",
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0",
        "Origin": "https://ats2.t2.ru",
        "Referer": "https://ats2.t2.ru/",
    }


def normalize_phone(phone: Any) -> str | None:
    if phone is None:
        return None
    digits = "".join(ch for ch in str(phone) if ch.isdigit())
    if digits.startswith("8") and len(digits) == 11:
        digits = "7" + digits[1:]
    return digits or None


def safe_json(response: httpx.Response) -> Any:
    try:
        return response.json()
    except ValueError:
        return response.text


@asynccontextmanager
async def lifespan(app: FastAPI):
    global tele2_client, moysklad_client

    logger.info("🚀 Запуск Tele2-MoySklad Middleware")
    tele2_client = httpx.AsyncClient(
        http2=True,
        follow_redirects=True,
        timeout=httpx.Timeout(
            connect=20.0, read=90.0, write=30.0, pool=30.0
        ),
    )
    moysklad_client = httpx.AsyncClient(
        timeout=httpx.Timeout(15.0),
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
    title="Tele2 - MoySklad Middleware",
    lifespan=lifespan,
)


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
        if response.status_code == 200:
            data = safe_json(response)
            if isinstance(data, dict) and data.get("accessToken"):
                TELE2_ACCESS_TOKEN = data["accessToken"]
                logger.info("🟢 Access Token Tele2 обновлён")
                return True

        logger.error(
            "Ошибка refresh Tele2 %s %s",
            response.status_code,
            response.text[:300],
        )
    except Exception:
        logger.exception("Ошибка обновления Tele2 token")
    return False


async def get_active_calls() -> Any:
    if tele2_client is None:
        return []

    url = f"{TELE2_API_URL}/monitoring/calls"
    try:
        response = await tele2_client.get(
            url,
            headers=get_t2_headers(TELE2_ACCESS_TOKEN),
        )
        if response.status_code == 401:
            if await refresh_tele2_token():
                response = await tele2_client.get(
                    url,
                    headers=get_t2_headers(TELE2_ACCESS_TOKEN),
                )

        if response.status_code == 200:
            return safe_json(response)

        logger.error(
            "Tele2 monitoring error %s %s",
            response.status_code,
            response.text[:300],
        )
    except Exception:
        logger.exception("Ошибка получения звонков Tele2")
    return []


async def find_customer_in_moysklad(phone: Any) -> dict | None:
    if not MOYSKLAD_TOKEN or moysklad_client is None:
        logger.warning("Нет MOYSKLAD_TOKEN или HTTP-клиента")
        return None

    clean_phone = normalize_phone(phone)
    if not clean_phone:
        return None

    url = f"{MOYSKLAD_API_URL}/entity/counterparty"
    headers = {
        "Accept": "application/json",
        "Authorization": f"Bearer {MOYSKLAD_TOKEN}",
    }
    params = {"filter": f"phone={clean_phone}"}

    try:
        response = await moysklad_client.get(
            url,
            headers=headers,
            params=params,
        )
        if response.status_code != 200:
            logger.error(
                "Ошибка МойСклад %s %s",
                response.status_code,
                response.text[:300],
            )
            return None

        data = safe_json(response)
        rows = data.get("rows", []) if isinstance(data, dict) else []
        return rows[0] if rows else None
    except Exception:
        logger.exception("Ошибка поиска клиента в МойСклад")
        return None


async def process_incoming_call(caller_phone: Any, call_id: Any) -> None:
    if not caller_phone or not call_id:
        return

    call_key = str(call_id)
    if is_processed(call_key):
        logger.info("Дубликат звонка %s", call_key)
        return

    customer = await find_customer_in_moysklad(caller_phone)
    if customer:
        logger.info(
            "✅ Клиент найден: %s | %s",
            customer.get("name"),
            caller_phone,
        )
    else:
        logger.info("ℹ️ Новый номер: %s", caller_phone)


@app.post("/api/tele2/webhook")
async def tele2_webhook(
    request: Request,
    background_tasks: BackgroundTasks,
):
    try:
        data = await request.json()
        logger.info("📥 Tele2 webhook: %s", data)

        call = data.get("call") or {}
        caller = (
            data.get("caller")
            or data.get("from")
            or data.get("phone")
            or call.get("caller")
            or call.get("from")
        )
        call_id = (
            data.get("callId")
            or data.get("id")
            or call.get("callId")
            or call.get("id")
        )

        if caller and call_id:
            background_tasks.add_task(
                process_incoming_call,
                caller,
                str(call_id),
            )
        return {"status": "ok"}
    except Exception as exc:
        logger.exception("Ошибка Tele2 webhook")
        return JSONResponse(
            status_code=400,
            content={"status": "error", "detail": str(exc)},
        )


@app.post("/api/make-call")
async def make_outgoing_call(
    payload: dict,
    x_api_key: str | None = Header(default=None),
):
    if CALL_API_KEY and x_api_key != CALL_API_KEY:
        return JSONResponse(
            status_code=401,
            content={"status": "error", "message": "Unauthorized"},
        )

    if tele2_client is None:
        return JSONResponse(
            status_code=503,
            content={"status": "error", "message": "HTTP client not ready"},
        )

    phone = payload.get("phone") or payload.get("destination")
    source = payload.get("source") or payload.get("user")
    clean_phone = normalize_phone(phone)

    if not clean_phone:
        return JSONResponse(
            status_code=400,
            content={"status": "error", "message": "Некорректный номер"},
        )
    if not source:
        return JSONResponse(
            status_code=400,
            content={"status": "error", "message": "Не указан source"},
        )

    url = f"{TELE2_API_URL}/call/outgoing"
    body = {
        "destination": clean_phone,
        "source": str(source),
    }

    try:
        response = await tele2_client.post(
            url,
            headers=get_t2_headers(TELE2_ACCESS_TOKEN),
            json=body,
        )
        if response.status_code == 401 and await refresh_tele2_token():
            response = await tele2_client.post(
                url,
                headers=get_t2_headers(TELE2_ACCESS_TOKEN),
                json=body,
            )

        if response.status_code in (200, 201, 202):
            logger.info("📞 Исходящий звонок %s -> %s", source, clean_phone)
            return {"status": "success", "data": safe_json(response)}

        return JSONResponse(
            status_code=502,
            content={
                "status": "error",
                "tele2_status": response.status_code,
                "detail": safe_json(response),
            },
        )
    except Exception as exc:
        logger.exception("Ошибка исходящего звонка")
        return JSONResponse(
            status_code=500,
            content={"status": "error", "detail": str(exc)},
        )


@app.get("/api/test-tele2")
async def test_tele2():
    result = await get_active_calls()
    return {"status_code": 200, "body": result}


@app.get("/")
@app.head("/")
async def root():
    return {
        "status": "running",
        "service": "Tele2-MoySklad",
        "mode": "webhook",
    }


@app.get("/health")
async def health():
    return {"status": "ok"}
