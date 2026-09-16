import os
import requests
from fastapi import FastAPI, Request, HTTPException, BackgroundTasks

app = FastAPI()

# Переменные берутся из настроек сервера (мы укажем их позже)
MS_PHONE_API_KEY = os.getenv("MS_PHONE_API_KEY", "")
T2_API_TOKEN = os.getenv("T2_API_TOKEN", "")
T2_API_URL = os.getenv("T2_API_URL", "https://vats.t2.ru/api/v1")

# ----------------------------------------------------
# 1. Прием Click-to-Call от МоегоСклада -> отправка в T2
# ----------------------------------------------------
@app.post("/webhooks/moysklad/click-to-call")
async def ms_click_to_call(request: Request):
    try:
        data = await request.json()
        target_phone = data.get("phone")          # Номер клиента
        employee_ext = data.get("extension")      # Добавочный сотрудника

        # Запрос в АТС T2 на совершение вызова
        t2_payload = {
            "from_extension": employee_ext,
            "to_phone": target_phone
        }
        headers = {
            "X-API-TOKEN": T2_API_TOKEN,
            "Content-Type": "application/json"
        }
        
        # Отправляем запрос в ВАТС T2
        response = requests.post(f"{T2_API_URL}/make_call", json=t2_payload, headers=headers)
        
        return {"status": "success", "t2_response": response.text}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ----------------------------------------------------
# 2. Прием Webhook от T2 -> отправка карточки в МойСклад
# ----------------------------------------------------
@app.post("/webhooks/t2")
async def t2_webhook(request: Request):
    data = await request.json()
    event_type = data.get("event")
    
    if event_type == "incoming":
        # Уведомляем МойСклад о входящем
        requests.post(
            "https://online.moysklad.ru/api/remap/1.2/phone/v1/events",
            json={
                "event": "INCOMING",
                "phone": data.get("from"),
                "extension": data.get("to_extension"),
                "callID": data.get("call_id")
            },
            headers={"Authorization": f"Bearer {MS_PHONE_API_KEY}"}
        )
    return {"status": "ok"}