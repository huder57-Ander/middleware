import os
import asyncio
import httpx
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, BackgroundTasks

# --- АСИНХРОННАЯ РЕГИСТРАЦИЯ ВЕБХУКА ---

async def register_tele2_webhook_async():
    """Асинхронная регистрация Webhook без блокировки старта Uvicorn"""
    global TELE2_ACCESS_TOKEN
    
    if not TELE2_ACCESS_TOKEN:
        print("⚠️ TELE2_ACCESS_TOKEN не задан в Environment Variables. Регистрация Webhook пропущена.")
        return

    url = f"{TELE2_API_URL}/subscription/events"
    headers = get_t2_headers(TELE2_ACCESS_TOKEN)
    body = {
        "url": WEBHOOK_URL,
        "events": ["CALL_START", "CALL_END", "CALL_ANSWER"]
    }

    # Используем асинхронный клиент с увеличенным таймаутом (30 секунд)
    async with httpx.AsyncClient(http2=True, follow_redirects=True, timeout=30.0) as client:
        try:
            print("⏳ Отправка запроса на регистрацию Webhook в Tele2...")
            res = await client.post(url, headers=headers, json=body)
            
            # Если 401 или 403 — пробуем автоматически обновить токен
            if res.status_code in (401, 403):
                print("⚠️ Получена ошибка 403/401. Пробуем обновить Access Token через Refresh...")
                if refresh_tele2_token():
                    headers = get_t2_headers(TELE2_ACCESS_TOKEN)
                    res = await client.post(url, headers=headers, json=body)

            if res.status_code in (200, 201):
                print(f"✅ Webhook успешно зарегистрирован в Tele2: {WEBHOOK_URL}")
            else:
                print(f"⚠️ Ошибка регистрации Webhook: Status {res.status_code} | Ответ: {res.text[:200]}")
        except httpx.TimeoutException:
            print("🔴 Ошибка: Сервер Tele2 не ответил вовремя ( Read Timeout ). Проверьте доступность API Tele2.")
        except Exception as e:
            print(f"🔴 Исключение при подписке на Webhook: {e}")

# --- LIFESPAN (СТАРТ И ОСТАНОВКА) ---

@asynccontextmanager
async def lifespan(app: FastAPI):
    print("🚀 Сервер запускается...")
    # asyncio.create_task(register_tele2_webhook_async())  # Отключено из-за блока IP Render со стороны T2
    yield
    print("🛑 Сервер останавливается...")

app = FastAPI(title="Tele2 - MoySklad Middleware", lifespan=lifespan)

# --- КОНФИГУРАЦИЯ ---

TELE2_API_URL = os.getenv("TELE2_API_URL", "https://ats2.tele2.ru/crm/openapi").strip().strip("[]'\"").rstrip("/")
TELE2_ACCESS_TOKEN = os.getenv("TELE2_ACCESS_TOKEN", "").strip()
TELE2_REFRESH_TOKEN = os.getenv("TELE2_REFRESH_TOKEN", "").strip()

MOYSKLAD_API_URL = "https://api.moysklad.ru/api/remap/1.2"
MOYSKLAD_TOKEN = os.getenv("MOYSKLAD_TOKEN", "").strip()

WEBHOOK_URL = "https://middleware-hudia.onrender.com/api/tele2/webhook"

processed_calls = set()

# Постоянная сессия с поддержкой HTTP/2 для исходящих вызовов
t2_client = httpx.Client(http2=True, follow_redirects=True, timeout=15.0)

def get_t2_headers(token: str):
    """Форматирование заголовков под спецификацию авторизации Tele2 (без слова Bearer)"""
    clean_token = token.replace("Bearer ", "").strip()
    return {
        "Authorization": clean_token,
        "Accept": "application/json",
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    }

def refresh_tele2_token():
    """Обновление Access Token с помощью Refresh Token"""
    global TELE2_ACCESS_TOKEN
    url = f"{TELE2_API_URL}/authorization/refresh/token"
    headers = get_t2_headers(TELE2_REFRESH_TOKEN)
    try:
        response = t2_client.put(url, headers=headers)
        if response.status_code == 200:
            data = response.json()
            TELE2_ACCESS_TOKEN = data.get("accessToken", TELE2_ACCESS_TOKEN)
            print("🟢 Access Token T2 успешно обновлен!")
            return True
        else:
            print(f"🔴 Ошибка обновления токена T2: Status {response.status_code} | {response.text[:150]}")
    except Exception as e:
        print(f"🔴 Исключение при обновлении токена T2: {e}")
    return False

# --- ПОИСК В МОЕМСКЛАДЕ ---

def process_incoming_call_task(caller_phone: str, call_id: str):
    if not caller_phone or call_id in processed_calls:
        return
    
    processed_calls.add(call_id)
    if len(processed_calls) > 500:
        processed_calls.clear()

    if not MOYSKLAD_TOKEN:
        print("⚠️ MOYSKLAD_TOKEN не задан в переменные окружения.")
        return

    clean_phone = "".join(filter(str.isdigit, str(caller_phone)))
    url = f"{MOYSKLAD_API_URL}/entity/counterparty?filter=phone={clean_phone}"
    headers = {
        "Authorization": f"Bearer {MOYSKLAD_TOKEN}",
        "Content-Type": "application/json",
        "Accept": "application/json"
    }

    try:
        with httpx.Client(timeout=10.0) as client:
            res = client.get(url, headers=headers)
            if res.status_code == 200:
                rows = res.json().get("rows", [])
                if rows:
                    print(f"✅ Найден клиент в МоемСкладе: {rows[0].get('name')} ({clean_phone})")
                else:
                    print(f"ℹ️ Клиент {clean_phone} не найден в МоемСкладе.")
            else:
                print(f"🔴 Ошибка МойСклад API: Status {res.status_code}")
    except Exception as e:
        print(f"🔴 Ошибка при запросе к МоемуСкладу: {e}")

# --- ЭНДПОИНТЫ API ---

@app.post("/api/tele2/webhook")
async def tele2_webhook(request: Request, background_tasks: BackgroundTasks):
    """Прием push-событий о звонках от Tele2"""
    try:
        data = await request.json()
        print(f"📥 Получен Webhook от Tele2: {data}")
        
        caller = data.get("caller") or data.get("from")
        call_id = data.get("callId") or data.get("id")
        
        if caller and call_id:
            background_tasks.add_task(process_incoming_call_task, caller, str(call_id))
            
        return {"status": "ok"}
    except Exception as e:
        print(f"⚠️ Ошибка обработки Webhook T2: {e}")
        return {"status": "error"}, 400

@app.post("/api/make-call")
def make_outgoing_call(payload: dict):
    """Инициализация исходящего вызова из МоегоСклада"""
    global TELE2_ACCESS_TOKEN
    target_phone = payload.get("phone")
    user_extension = payload.get("user")

    if not target_phone:
        return {"status": "error", "message": "Не указан номер телефона"}, 400

    clean_phone = "".join(filter(str.isdigit, str(target_phone)))
    url = f"{TELE2_API_URL}/calls/outgoing"
    headers = get_t2_headers(TELE2_ACCESS_TOKEN)
    body = {"phone": clean_phone, "user": user_extension}

    try:
        response = t2_client.post(url, headers=headers, json=body)
        
        if response.status_code in (401, 403):
            if refresh_tele2_token():
                headers = get_t2_headers(TELE2_ACCESS_TOKEN)
                response = t2_client.post(url, headers=headers, json=body)

        if response.status_code in (200, 201, 202):
            print(f"📞 Инициирован исходящий звонок: {user_extension} -> {clean_phone}")
            return {"status": "success", "data": response.json()}
        else:
            return {"status": "error", "code": response.status_code, "detail": response.text}, 400
    except Exception as e:
        return {"status": "error", "detail": str(e)}, 500

# --- СЛУЖЕБНЫЕ ЭНДПОИНТЫ ---

@app.api_route("/", methods=["GET", "HEAD", "POST"])
def read_root():
    return {"status": "running", "mode": "webhook"}

@app.get("/health")
def health_check():
    return {"status": "ok"}
