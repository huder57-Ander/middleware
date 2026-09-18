import os
import logging
from contextlib import asynccontextmanager
from typing import Optional

import httpx
from cachetools import TTLCache

from fastapi import FastAPI, Request, BackgroundTasks, Header
from fastapi.responses import JSONResponse


# ==========================================================
# LOGGING
# ==========================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger("tele2-moysklad")


# ==========================================================
# CONFIGURATION
# ==========================================================

TELE2_API_URL = (
    os.getenv(
        "TELE2_API_URL",
        "https://ats2.tele2.ru/crm/openapi"
    )
    .strip()
    .strip("[]'\"")
    .rstrip("/")
)

TELE2_ACCESS_TOKEN = os.getenv(
    "TELE2_ACCESS_TOKEN",
    ""
).strip()

TELE2_REFRESH_TOKEN = os.getenv(
    "TELE2_REFRESH_TOKEN",
    ""
).strip()


MOYSKLAD_API_URL = (
    "https://api.moysklad.ru/api/remap/1.2"
)

MOYSKLAD_TOKEN = os.getenv(
    "MOYSKLAD_TOKEN",
    ""
).strip()


WEBHOOK_URL = os.getenv(
    "WEBHOOK_URL",
    "https://middleware-hudia.onrender.com/api/tele2/webhook"
)


# Необязательно.
# Если переменная задана - включается защита /api/make-call

CALL_API_KEY = os.getenv(
    "CALL_API_KEY",
    ""
).strip()


# ==========================================================
# GLOBAL CLIENTS
# ==========================================================

tele2_client: Optional[httpx.AsyncClient] = None
moysklad_client: Optional[httpx.AsyncClient] = None


# ==========================================================
# CACHE CALLS
# ==========================================================

processed_calls = TTLCache(
    maxsize=10000,
    ttl=86400
)


# ==========================================================
# TELE2 AUTH HEADERS
# ВАЖНО:
# Tele2 используется без Bearer
# ==========================================================

def get_t2_headers(token: str):

    clean_token = (
        token
        .replace("Bearer ", "")
        .strip()
    )

    return {
        "Authorization": clean_token,
        "Accept": "application/json",
        "Content-Type": "application/json",
        "User-Agent":
            "Mozilla/5.0"
    }


# ==========================================================
# PHONE NORMALIZATION
# ==========================================================

def normalize_phone(phone):

    if not phone:
        return None

    digits = "".join(
        filter(
            str.isdigit,
            str(phone)
        )
    )

    if digits.startswith("8"):
        digits = "7" + digits[1:]

    return digits


# ==========================================================
# FASTAPI LIFESPAN
# ==========================================================

@asynccontextmanager
async def lifespan(app: FastAPI):

    global tele2_client
    global moysklad_client

    logger.info(
        "🚀 Starting middleware..."
    )

    tele2_client = httpx.AsyncClient(
        http2=True,
        follow_redirects=True,
        timeout=20.0
    )

    moysklad_client = httpx.AsyncClient(
        timeout=15.0
    )


    yield


    logger.info(
        "🛑 Stopping middleware..."
    )


    if tele2_client:
        await tele2_client.aclose()


    if moysklad_client:
        await moysklad_client.aclose()



app = FastAPI(
    title="Tele2 - MoySklad Middleware",
    lifespan=lifespan
)
# ==========================================================
# TELE2 TOKEN REFRESH
# ==========================================================

async def refresh_tele2_token():

    global TELE2_ACCESS_TOKEN

    if not TELE2_REFRESH_TOKEN:
        logger.error(
            "Нет TELE2_REFRESH_TOKEN"
        )
        return False


    url = (
        f"{TELE2_API_URL}"
        "/authorization/refresh/token"
    )


    headers = get_t2_headers(
        TELE2_REFRESH_TOKEN
    )


    try:

        response = await tele2_client.put(
            url,
            headers=headers
        )


        if response.status_code == 200:

            data = response.json()

            TELE2_ACCESS_TOKEN = data.get(
                "accessToken",
                TELE2_ACCESS_TOKEN
            )


            logger.info(
                "🟢 Tele2 Access Token обновлен"
            )

            return True


        logger.error(
            "Ошибка обновления токена Tele2 %s %s",
            response.status_code,
            response.text[:200]
        )


    except Exception as e:

        logger.exception(
            "Ошибка refresh token Tele2: %s",
            e
        )


    return False



# ==========================================================
# REGISTER TELE2 WEBHOOK
# ==========================================================

async def register_tele2_webhook():

    if not TELE2_ACCESS_TOKEN:

        logger.warning(
            "Нет TELE2_ACCESS_TOKEN. Webhook не зарегистрирован"
        )

        return False



    url = (
        f"{TELE2_API_URL}"
        "/subscription/events"
    )


    headers = get_t2_headers(
        TELE2_ACCESS_TOKEN
    )


    payload = {

        "url": WEBHOOK_URL,

        "events": [
            "CALL_START",
            "CALL_END",
            "CALL_ANSWER"
        ]
    }


    try:

        response = await tele2_client.post(
            url,
            headers=headers,
            json=payload
        )


        if response.status_code in (
            401,
            403
        ):

            logger.warning(
                "Tele2 token expired"
            )

            if await refresh_tele2_token():

                headers = get_t2_headers(
                    TELE2_ACCESS_TOKEN
                )

                response = await tele2_client.post(
                    url,
                    headers=headers,
                    json=payload
                )


        if response.status_code in (
            200,
            201
        ):

            logger.info(
                "✅ Tele2 webhook зарегистрирован"
            )

            return True


        logger.error(
            "Ошибка регистрации webhook %s %s",
            response.status_code,
            response.text[:300]
        )


    except Exception as e:

        logger.exception(
            "Ошибка webhook registration: %s",
            e
        )


    return False



# ==========================================================
# MOYSKLAD SEARCH
# ==========================================================

async def find_customer_in_moysklad(
    phone: str
):

    if not MOYSKLAD_TOKEN:

        logger.warning(
            "Нет MOYSKLAD_TOKEN"
        )

        return None



    clean_phone = normalize_phone(
        phone
    )


    if not clean_phone:
        return None



    url = (
        f"{MOYSKLAD_API_URL}"
        "/entity/counterparty"
    )


    params = {

        "filter":
            f"phone={clean_phone}"
    }


    headers = {

        "Authorization":
            f"Bearer {MOYSKLAD_TOKEN}",

        "Accept":
            "application/json"
    }



    try:

        response = await moysklad_client.get(
            url,
            headers=headers,
            params=params
        )


        if response.status_code == 200:

            data = response.json()

            rows = data.get(
                "rows",
                []
            )


            if rows:

                return rows[0]


            return None



        logger.error(
            "МойСклад ошибка %s %s",
            response.status_code,
            response.text[:200]
        )


    except Exception as e:

        logger.exception(
            "Ошибка поиска МойСклад: %s",
            e
        )


    return None



# ==========================================================
# PROCESS INCOMING CALL
# ==========================================================

async def process_incoming_call(
    caller_phone: str,
    call_id: str
):

    if not caller_phone or not call_id:
        return



    if call_id in processed_calls:

        logger.info(
            "Дубликат звонка %s",
            call_id
        )

        return



    processed_calls[call_id] = True


    customer = await find_customer_in_moysklad(
        caller_phone
    )


    if customer:

        logger.info(
            "✅ Клиент найден: %s | %s",
            customer.get("name"),
            caller_phone
        )

    else:

        logger.info(
            "ℹ️ Новый номер: %s",
            caller_phone
        )
        # ==========================================================
# TELE2 WEBHOOK
# ==========================================================

@app.post("/api/tele2/webhook")
async def tele2_webhook(
    request: Request,
    background_tasks: BackgroundTasks
):

    try:

        data = await request.json()

        logger.info(
            "📥 Tele2 webhook: %s",
            data
        )


        # Поддержка разных вариантов JSON Tele2

        call_data = data.get(
            "call",
            {}
        )


        caller = (

            data.get("caller")

            or data.get("from")

            or data.get("phone")

            or call_data.get("caller")

            or call_data.get("from")

        )


        call_id = (

            data.get("callId")

            or data.get("id")

            or call_data.get("callId")

            or call_data.get("id")

        )


        if caller and call_id:

            background_tasks.add_task(

                process_incoming_call,

                caller,

                str(call_id)

            )


        return {
            "status": "ok"
        }


    except Exception as e:

        logger.exception(
            "Ошибка обработки Tele2 webhook: %s",
            e
        )


        return JSONResponse(

            status_code=400,

            content={

                "status": "error",

                "detail": str(e)

            }

        )



# ==========================================================
# OUTGOING CALL FROM MOYSKLAD
# ==========================================================

@app.post("/api/make-call")
async def make_outgoing_call(

    payload: dict,

    x_api_key: str | None = Header(
        default=None
    )

):


    # Если задан CALL_API_KEY,
    # включаем проверку

    if CALL_API_KEY:

        if x_api_key != CALL_API_KEY:

            return JSONResponse(

                status_code=401,

                content={

                    "status":"error",

                    "message":
                    "Unauthorized"

                }

            )



    global TELE2_ACCESS_TOKEN


    phone = payload.get(
        "phone"
    )

    user = payload.get(
        "user"
    )



    if not phone:

        return JSONResponse(

            status_code=400,

            content={

                "status":"error",

                "message":
                "Не указан телефон"

            }

        )



    clean_phone = normalize_phone(
        phone
    )



    if not clean_phone:

        return JSONResponse(

            status_code=400,

            content={

                "status":"error",

                "message":
                "Неверный номер"

            }

        )



    url = (
        f"{TELE2_API_URL}"
        "/calls/outgoing"
    )


    body = {

        "phone":
            clean_phone,

        "user":
            user

    }



    try:

        headers = get_t2_headers(
            TELE2_ACCESS_TOKEN
        )


        response = await tele2_client.post(

            url,

            headers=headers,

            json=body

        )



        if response.status_code in (
            401,
            403
        ):


            if await refresh_tele2_token():

                headers = get_t2_headers(
                    TELE2_ACCESS_TOKEN
                )


                response = await tele2_client.post(

                    url,

                    headers=headers,

                    json=body

                )



        if response.status_code in (

            200,
            201,
            202

        ):


            logger.info(

                "📞 Исходящий звонок %s -> %s",

                user,

                clean_phone

            )


            return {

                "status":
                    "success",

                "data":
                    response.json()

            }



        return JSONResponse(

            status_code=400,

            content={

                "status":
                    "error",

                "code":
                    response.status_code,

                "detail":
                    response.text

            }

        )



    except Exception as e:


        logger.exception(
            "Ошибка исходящего звонка: %s",
            e
        )


        return JSONResponse(

            status_code=500,

            content={

                "status":
                    "error",

                "detail":
                    str(e)

            }

        )



# ==========================================================
# MANUAL WEBHOOK REGISTER
# ==========================================================

@app.post("/api/register-webhook")
async def manual_register_webhook():

    result = await register_tele2_webhook()


    return {

        "registered":
            result,

        "webhook":
            WEBHOOK_URL

    }



# ==========================================================
# SERVICE ENDPOINTS
# ==========================================================

@app.api_route(
    "/",
    methods=[
        "GET",
        "HEAD",
        "POST"
    ]
)
async def root():

    return {

        "status":
            "running",

        "service":
            "Tele2-MoySklad",

        "mode":
            "webhook"

    }



@app.get("/health")
async def health():

    return {

        "status":
            "ok"

    }
