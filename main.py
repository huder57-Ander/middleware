import os
import time
import threading
import requests
from fastapi import FastAPI

app = FastAPI(title="Tele2 - MoySklad Middleware")

# --- КОНФИГУРАЦИЯ (Загрузка из переменной окружения Render) ---
TELE2_API_URL = os.getenv("TELE2_API_URL", "https://ats2.tele2.ru/crm/openapi")
TELE2_ACCESS_TOKEN = os.getenv("TELE2_ACCESS_TOKEN", "")
TELE2_REFRESH_TOKEN = os.getenv("TELE2_REFRESH_TOKEN", "")

MOYSKLAD_API_URL = "https://api.moysklad.ru/api/remap/1.2"
MOYSKLAD_TOKEN = os.getenv("MOYSKLAD_TOKEN", "")

processed_calls = set()

# --- ФУНКЦИИ ВЗАИМОДЕЙСТВИЯ С КАТС T2 ---

def refresh_tele2_token():
    """Обновление просроченного Access Token через Refresh Token"""
    global TELE2_ACCESS_TOKEN
    url = f"{TELE2_API_URL}/authorization/refresh/token"
    headers = {
        "Authorization": TELE2_REFRESH_TOKEN,
        "Accept": "application/json"
    }
    
    try:
        response = requests.put(url, headers=headers, timeout=10)
        if response.status_code == 200:
            data = response.json()
            TELE2_ACCESS_TOKEN = data.get("accessToken", TELE2_ACCESS_TOKEN)
            print("🟢 Access Token T2 успешно обновлен!")
            return True
        else:
            print(f"🔴 Ошибка обновления токена T2: Status {response.status_code}, {response.text}")
            return False
    except Exception as e:
        print(f"🔴 Исключение при обновлении токена T2: {e}")
        return False


def get_active_calls():
    """Запрос активных звонков из КАТС T2 (GET /monitoring/calls)"""
    global TELE2_ACCESS_TOKEN
    url = f"{TELE2_API_URL}/monitoring/calls"
    headers = {
        "Authorization": TELE2_ACCESS_TOKEN,
        "Accept": "application/json",
        "User-Agent": "Mozilla/5.0"
    }

    try:
        response = requests.get(url, headers=headers, timeout=10)
        
        # Если токен истек (401 или 403)
        if response.status_code in (401, 403):
            print("⚠️ Access Token просрочен. Обновляем...")
            if refresh_tele2_token():
                headers["Authorization"] = TELE2_ACCESS_TOKEN
                response = requests.get(url, headers=headers, timeout=10)

        if response.status_code == 200:
            return response.json()
        else:
            print(f"⚠️ Ошибка получения звонков: Status {response.status_code}")
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
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code == 200:
            data = response.json()
            rows = data.get("rows", [])
            if rows:
                client_name = rows[0].get("name")
                print(f"✅ Найден клиент в МоемСкладе: {client_name} ({caller_phone})")
            else:
                print(f"ℹ️ Клиент с номером {caller_phone} не найден в МоемСкладе.")
        else:
            print(f"🔴 Ошибка МойСклад API: {response.status_code}")
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
