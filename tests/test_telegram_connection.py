import os
import requests

token = os.environ["TELEGRAM_BOT_TOKEN"]
chat_id = os.environ["TELEGRAM_CHAT_ID"]

print("TELEGRAM_CHAT_ID =", chat_id)
print("Token length =", len(token))

me = requests.get(
    f"https://api.telegram.org/bot{token}/getMe",
    timeout=30,
)
print("getMe:", me.status_code, me.text)

response = requests.post(
    f"https://api.telegram.org/bot{token}/sendMessage",
    json={
        "chat_id": chat_id,
        "text": "✅ Test message from Airflow DAG04 Telegram setup.",
    },
    timeout=30,
)
print("sendMessage:", response.status_code, response.text)