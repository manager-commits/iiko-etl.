import os
import datetime as dt
import requests
import psycopg2
from dotenv import load_dotenv

# Загружаем переменные окружения (.env)
load_dotenv()

# Настройки iiko (берём из секретов GitHub)
IIKO_BASE_URL = os.getenv("IIKO_BASE_URL", "").rstrip("/")
IIKO_LOGIN = os.getenv("IIKO_LOGIN")
IIKO_PASSWORD = os.getenv("IIKO_PASSWORD")

# Подключение к Postgres (Neon)
def get_pg_connection():
    return psycopg2.connect(
        host=os.getenv("PG_HOST"),
        port=os.getenv("PG_PORT"),
        dbname=os.getenv("PG_DB"),
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD"),
        sslmode=os.getenv("PG_SSLMODE", "require")
    )

# Функция получения токена от iiko
def get_token():
    url = f"{IIKO_BASE_URL}/api/auth"
    params = {"login": IIKO_LOGIN, "pass": IIKO_PASSWORD}

    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()

    token = resp.text.strip()
    print(f"🔑 Токен получен: {token[:6]}...")
    return token

# Корректный logout
def logout(token: str):
    url = f"{IIKO_BASE_URL}/api/logout"
    params = {"key": token}
    try:
        requests.post(url, params=params, timeout=10)
    except Exception as e:
        print("⚠️ Ошибка при logout:", e)

# Работа с периодом выгрузки
def get_period():
    date_from_str = os.getenv("DATE_FROM")
    date_to_str = os.getenv("DATE_TO")

    if date_from_str and date_to_str:
        date_from = dt.date.fromisoformat(date_from_str)
        date_to = dt.date.fromisoformat(date_to_str)
        print(f"📅 Используем период из ENV: {date_from} – {date_to}")
        return date_from, date_to

    # По умолчанию — вчера
    today = dt.date.today()
    date_from = today - dt.timedelta(days=1)
    date_to = today - dt.timedelta(days=1)
    print(f"📅 Используем период по умолчанию: {date_from}")
    return date_from, date_to

# Заглушка — здесь позже будет запрос OLAP
def fetch_t1_light(token, date_from, date_to):
    print("📡 Загружаем данные TI Light из iiko...")

    url = f"{IIKO_BASE_URL}/api/v2/reports/olap"
    params = {"key": token}

    body = {
        "reportType": "SALES",
        "buildSummary": False,
        "groupByRowFields": [
            "Delivery.CookingFinishTime",
            "OpenTime",
            "Delivery.PrintTime",
            "Delivery.SendTime",
            "Delivery.ActualTime",
            "Delivery.CloseTime",
            "Delivery.ExpectedTime",
            "OpenDate.Typed",
            "Delivery.SourceKey",
            "Delivery.DeliveryComment",
            "Department",
            "Delivery.Region",
            "Delivery.Number",
            "Delivery.CustomerName",
            "Delivery.Phone",
            "Delivery.Address",
            "Delivery.Courier"
        ],
        "aggregateFields": [],

        "filters": {
            "SessionID.OperDay": {
                "filterType": "DateRange",
                "periodType": "CUSTOM",
                "from": date_from.strftime("%Y-%m-%d"),
                "to": date_to.strftime("%Y-%m-%d"),
                "includeLow": True,
                "includeHigh": False
            },
            "Storned": {
                "filterType": "IncludeValues",
                "values": ["FALSE"]
            },
            "DeletedWithWriteoff": {
                "filterType": "IncludeValues",
                "values": ["NOT_DELETED"]
            },
            "Department": {
                "filterType": "IncludeValues",
                "values": ["Авиагородок", "Домодедово"]
            },
            "OrderDeleted": {
                "filterType": "IncludeValues",
                "values": ["NOT_DELETED"]
            },
            "Delivery.CookingFinishTime": {
                "filterType": "ExcludeValues",
                "values": [None]
            },
            "Delivery.Courier": {
                "filterType": "ExcludeValues",
                "values": [None, "Самовывоз"]
            }
        }
    }

    resp = requests.post(url, params=params, json=body, timeout=90)
    resp.raise_for_status()

    print("✅ Данные получены")
    return resp.json()

# Заглушка — здесь позже будет запись в таблицу
def upsert_t1_light(data):
    """
    ВРЕМЕННАЯ версия:
    просто смотрим, что пришло от iiko, без записи в базу
    """
    rows = data.get("data", [])
    print(f"📊 Получено строк из отчёта: {len(rows)}")

    if not rows:
        print("⚠️ Отчёт пустой, нечего показывать.")
        return

    first = rows[0]
    print("🔎 Пример первой строки (ключи и значения):")
    for key, value in first.items():
        print(f"  {key}: {value}")

# Основной процесс ETL
def main():
    date_from, date_to = get_period()
    print(f"🚀 Старт ETL TI Light: {date_from} – {date_to}")

    token = get_token()
    try:
        data = fetch_t1_light(token, date_from, date_to)
        upsert_t1_light(data)
    finally:
        logout(token)
        print("🔐 Logout выполнен")

if __name__ == "__main__":
    main()
