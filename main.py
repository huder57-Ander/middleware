import os
import time
import logging
from contextlib import asynccontextmanager

import httpx

from fastapi import (
    FastAPI,
    Request,
    BackgroundTasks,
    Header
)

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



CALL_API_KEY = os.getenv(
    "CALL_API_KEY",
    ""
).strip()



# ==========================================================
# HTTP CLIENTS
# ==========================================================

tele2_client = None

moysklad_client = None



# ==========================================================
# SIMPLE TTL CACHE FOR CALL IDS
# ==========================================================

processed_calls = {}

CALL_CACHE_TTL = 86400


def is_processed(call_id: str):

    now = time.time()


    # удаляем старые звонки

    expired = [
        key
        for key, value in processed_calls.items()
        if now - value > CALL_CACHE_TTL
    ]


    for key in expired:
        del processed_calls[key]



    if call_id in processed_calls:
        return True



    processed_calls[call_id] = now



    # защита памяти

    if len(processed_calls) > 10000:

        first_key = next(
            iter(processed_calls)
        )

        del processed_calls[first_key]


    return False



# ==========================================================
# TELE2 HEADERS
# Авторизация без Bearer
# ==========================================================

def get_t2_headers(token: str):

    return {
        "Authorization": token.strip(),
        "Accept": "application/json"
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

        digits = (
            "7" +
            digits[1:]
        )


    return digits
# ==========================================================
# FASTAPI LIFESPAN
# ==========================================================

@asynccontextmanager
async def lifespan(app: FastAPI):

    global tele2_client
    global moysklad_client


    logger.info(
        "🚀 Запуск Tele2-MoySklad Middleware"
    )


    tele2_client = httpx.AsyncClient(
    http2=False,
    follow_redirects=True,
    timeout=httpx.Timeout(
        connect=20.0,
        read=90.0,
        write=30.0,
        pool=30.0
    )
)


    moysklad_client = httpx.AsyncClient(
        timeout=15.0
    )


    yield



    logger.info(
        "🛑 Остановка Middleware"
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
# REFRESH TELE2 TOKEN
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
                "🟢 Access Token Tele2 обновлен"
            )


            return True



        logger.error(

            "Ошибка refresh Tele2 %s %s",

            response.status_code,

            response.text[:200]

        )



    except Exception as e:

        logger.exception(

            "Ошибка обновления Tele2 token: %s",

            e

        )


    return False




# ==========================================================
# REGISTER TELE2 WEBHOOK
# ==========================================================

async def register_tele2_webhook():


    if not TELE2_ACCESS_TOKEN:

        logger.warning(

            "Нет TELE2_ACCESS_TOKEN"

        )

        return False



    url = (

        f"{TELE2_API_URL}"

        "/subscription/events"

    )



    headers = get_t2_headers(

        TELE2_ACCESS_TOKEN

    )



    body = {


        "url":

            WEBHOOK_URL,


        "events":

            [

                "CALL_START",

                "CALL_END",

                "CALL_ANSWER"

            ]

    }



    try:


        response = await tele2_client.post(

            url,

            headers=headers,

            json=body

        )



        if response.status_code in (

            401,

            403

        ):


            logger.warning(

                "Tele2 token просрочен"

            )


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

            201

        ):


            logger.info(

                "✅ Webhook Tele2 зарегистрирован"

            )


            return True



        logger.error(

            "Webhook ошибка %s %s",

            response.status_code,

            response.text[:300]

        )



    except Exception as e:


        logger.exception(

            "Ошибка регистрации webhook: %s",

            e

        )


    return False





# ==========================================================
# SEARCH CUSTOMER IN MOYSKLAD
# ==========================================================

async def find_customer_in_moysklad(phone):


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
    "Accept": "application/json",
    "Authorization": TELE2_ACCESS_TOKEN,
    "Origin": "https://ats2.t2.ru",
    "Referer": "https://ats2.t2.ru/",
    "User-Agent": "Mozilla/5.0"
}


    try:


        response = await moysklad_client.get(

            url,

            headers=headers,

            params=params

        )



        if response.status_code == 200:


            rows = response.json().get(

                "rows",

                []

            )



            if rows:

                return rows[0]



            return None



        logger.error(

            "Ошибка МойСклад %s %s",

            response.status_code,

            response.text[:200]

        )



    except Exception as e:


        logger.exception(

            "Ошибка поиска клиента: %s",

            e

        )



    return None





# ==========================================================
# PROCESS INCOMING CALL
# ==========================================================

async def process_incoming_call(

    caller_phone,

    call_id

):


    if not caller_phone or not call_id:

        return



    if is_processed(

        str(call_id)

    ):


        logger.info(

            "Дубликат звонка %s",

            call_id

        )


        return




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


        call = data.get(
            "call",
            {}
        )


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

                str(call_id)

            )



        return {

            "status":

                "ok"

        }



    except Exception as e:


        logger.exception(

            "Ошибка Tele2 webhook: %s",

            e

        )


        return JSONResponse(

            status_code=400,

            content={

                "status":

                    "error",

                "detail":

                    str(e)

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


    # Проверка ключа если включен

    if CALL_API_KEY:


        if x_api_key != CALL_API_KEY:


            return JSONResponse(

                status_code=401,

                content={

                    "status":

                        "error",

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

                "status":

                    "error",

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

                "status":

                    "error",

                "message":

                    "Некорректный номер"

            }

        )




    url = (

        f"{TELE2_API_URL}"

        "/call/outgoing"

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
# TEST TELE2 CONNECTION
# ==========================================================
@app.get("/api/test-tele2")
async def test_tele2():

    url = "https://ats2.t2.ru/crm/openapi/monitoring/calls"

    headers = {
        "Accept": "application/json",
        "Authorization": TELE2_ACCESS_TOKEN,
        "Origin": "https://ats2.t2.ru",
        "Referer": "https://ats2.t2.ru/",
        "User-Agent": "Mozilla/5.0"
    }

    params = {
        "page": 0,
        "size": 20
    }

    async with httpx.AsyncClient(
        http2=True,
        timeout=30
    ) as client:

        r = await client.get(
            url,
            headers=headers,
            params=params
        )

        return {
            "url": str(r.request.url),
            "status": r.status_code,
            "body": r.text[:500]
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
