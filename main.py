import os
import time
import threading
import httpx
from fastapi import FastAPI

app = FastAPI(title="Tele2 - MoySklad Middleware")

# --- КОНФИГУРАЦИЯ ---
TELE2_API_URL = os.getenv("TELE2_API_URL", "https://ats2.tele2.ru/crm/openapi").strip().strip("[]'\"").rstrip("/")
TELE2_ACCESS_TOKEN = os.getenv("TELE2_ACCESS_TOKEN", "").strip()
TELE2_REFRESH_TOKEN = os.getenv("TELE2_REFRESH_TOKEN", "").strip()

MOYSKLAD_API_URL = "https://api.moysklad.ru/api/remap/1.2"
MOYSKLAD_TOKEN = os.getenv("MOYSKLAD_TOKEN", "").strip()

processed_calls = set()

def get_t2_headers(token: str):
    clean_token = token.replace("Bearer ", "").strip()
    return {
        "Authorization": clean_token,
        "Accept": "application/json",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    }

# Создаем единую постоянную HTTP/2 сессию для обхода фильтров Nginx
t2_client = httpx.Client(http2=True, follow_redirects=True, timeout=12.0)

# --- ФУНКЦИИ ВЗАИМОДЕЙСТВИЯ С КАТС T2 ---

def refresh_tele2_token():
    """Обновление просроченного Access Token через Refresh Token"""
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
            print(f"🔴 Ошибка обновления токена T2: Status {response.status_code}, Body: {response.text[:150]}")
            return False
    except Exception as e:
        print(f"🔴 Исключение при обновлении токена T2: {e}")
        return False


def get_active_calls():
    """Запрос активных звонков из КАТС T2 (GET /monitoring/calls)"""
    global TELE2_ACCESS_TOKEN
    url = f"{TELE2_API_URL}/monitoring/calls"
    headers = get_t2_headers(TELE2_ACCESS_TOKEN)

    try:
        response = t2_client.get(url, headers=headers)
        
        # Если токен просрочен (401 или 403)
        if response.status_code in (401, 403):
            print("⚠️ Access Token просрочен или недействителен. Пробуем обновить...")
            if refresh_tele2_token():
                headers = get_t2_headers(TELE2_ACCESS_TOKEN)
                response = t2_client.get(url, headers=headers)

        if response.status_code == 200:
            return response.json()
        else:
            print(f"⚠️ Ошибка получения звонков: Status {response.status_code} | Ответ: {response.text[:150]}")
            return []
    except Exception as e:
        print(f"🔴 Ошибка сети при запросе к T2: {e}")
        return []

# --- ФУНКЦИИ ВЗАИМОДЕЙСТВИЯ С МОЙСКЛАД ---

def send_to_moysklad(call_data):
    """Поиск контрагента в МоемСкладе по номеру телефона"""
    caller_phone = call_data.get("caller")
    call_id = call_data.get("callId")

    if not caller_phone or call_id in processed_calls:
        return

    print(f"📞 Обработка входящего вызова {call_id} от {caller_phone}...")
    processed_calls.add(call_id)

    if len(processed_calls) > 500:
        processed_calls.clear()

    if not MOYSKLAD_TOKEN:
        print("⚠️ MOYSKLAD_TOKEN не задан в Environment Variables.")
        return

    url = f"{MOYSKLAD_API_URL}/entity/counterparty?filter=phone={caller_phone}"
    headers = {
        "Authorization": f"Bearer {MOYSKLAD_TOKEN}",
        "Content-Type": "application/json",
        "Accept": "application/json"
    }

    try:
        with httpx.Client(follow_redirects=True, timeout=10.0) as client:
            response = client.get(url, headers=headers)
            if response.status_code == 200:
                data = response.json()
                rows = data.get("rows", [])
                if rows:
                    client_name = rows[0].get("name")
                    print(f"✅ Найден клиент в МоемСкладе: {client_name} ({caller_phone})")
                else:
                    print(f"ℹ️ Клиент с номером {caller_phone} не найден в МоемСкладе.")
            else:
                print(f"🔴 Ошибка МойСклад API: Status {response.status_code}")
    except Exception as e:
        print(f"🔴 Ошибка отправки в МойСклад: {e}")

# --- ФОНОВЫЙ ПРОЦЕСС ОПРОСА ---

def poll_tele2_loop():
    """Фоновый цикл: запрашивает активные звонки каждые 3 секунды"""
    print("🚀 Запущен фоновый опрос КАТС T2...")
    while True:
        calls = get_active_calls()
        if calls:
            print(f"📲 Активные звонки в КАТС T2: {calls}")
            for call in calls:
                send_to_moysklad(call)
        time.sleep(3)

threading.Thread(target=poll_tele2_loop, daemon=True).start()

# --- ЭНДПОИНТЫ ДЛЯ RENDER И МОЕГОСКЛАДА ---

@app.api_route("/", methods=["GET", "HEAD"])
def read_root():
    return {"status": "running", "service": "Tele2 - MoySklad Middleware"}

@app.post("/api/moysklad")
def moysklad_webhook():
    return {"status": "received"}

@app.get("/health")
def health_check():
    return {"status": "ok"}
