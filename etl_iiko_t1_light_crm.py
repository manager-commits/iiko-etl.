import os
import datetime as dt
import requests
import psycopg2
from dotenv import load_dotenv

# Загружаем переменные окружения (.env) — локально полезно, в GitHub Actions тоже не мешает
load_dotenv()

# Настройки iiko (берём из секретов GitHub)
IIKO_BASE_URL = os.getenv("IIKO_BASE_URL", "").rstrip("/")
IIKO_LOGIN = os.getenv("IIKO_LOGIN")
IIKO_PASSWORD = os.getenv("IIKO_PASSWORD")


# Подключение к Postgres (Neon) — теперь через PG_CRM_*
def get_pg_connection():
    return psycopg2.connect(
        host=os.getenv("PG_CRM_HOST"),
        port=os.getenv("PG_CRM_PORT"),
        dbname=os.getenv("PG_CRM_DB"),
        user=os.getenv("PG_CRM_USER"),
        password=os.getenv("PG_CRM_PASSWORD"),
        sslmode=os.getenv("PG_CRM_SSLMODE", "require"),
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


def fetch_t1_light(token, date_from, date_to):
    print("📦 Загружаем данные TI Light из iiko...")

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
            "Delivery.Courier",
        ],
        "aggregateFields": [],
        "filters": {
            "OpenDate.Typed": {
                "filterType": "DateRange",
                "periodType": "CUSTOM",
                "from": date_from.strftime("%Y-%m-%d"),
                "to": date_to.strftime("%Y-%m-%d"),
                "includeLow": True,
                "includeHigh": True,
            },
            "Storned": {"filterType": "IncludeValues", "values": ["FALSE"]},
            "DeletedWithWriteoff": {"filterType": "IncludeValues", "values": ["NOT_DELETED"]},
            "Department": {"filterType": "IncludeValues", "values": ["Авиагородок", "Домодедово"]},
            "OrderDeleted": {"filterType": "IncludeValues", "values": ["NOT_DELETED"]},
            "Delivery.CookingFinishTime": {"filterType": "ExcludeValues", "values": [None]},
            "Delivery.Courier": {"filterType": "ExcludeValues", "values": [None, "Самовывоз"]},
        },
    }

    resp = requests.post(url, params=params, json=body, timeout=90)

    print("HTTP статус iiko:", resp.status_code)
    print("Тело ответа iiko (первые 1000 символов):")
    print(resp.text[:1000])

    resp.raise_for_status()

    print("✅ Данные получены")
    return resp.json()


def upsert_t1_light(data):
    print("💾 Запись данных в базу Neon...")

    rows = data.get("data", [])
    print(f"📊 Получено строк: {len(rows)}")

    if not rows:
        print("⚠️ Нет данных для записи")
        return

    conn = get_pg_connection()
    cur = conn.cursor()

    # ВАЖНО: пишем именно в crm.iiko_t1_light
    query = """
    INSERT INTO crm.iiko_t1_light (
        delivery_cooking_finish_time,
        open_time,
        delivery_print_time,
        delivery_send_time,
        delivery_actual_time,
        delivery_close_time,
        delivery_expected_time,
        open_date,
        delivery_source_key,
        delivery_comment,
        department,
        delivery_region,
        delivery_number,
        delivery_customer_name,
        delivery_phone,
        delivery_address,
        delivery_courier,
        updated_at
    )
    VALUES (
        %(Delivery.CookingFinishTime)s,
        %(OpenTime)s,
        %(Delivery.PrintTime)s,
        %(Delivery.SendTime)s,
        %(Delivery.ActualTime)s,
        %(Delivery.CloseTime)s,
        %(Delivery.ExpectedTime)s,
        %(OpenDate.Typed)s,
        %(Delivery.SourceKey)s,
        %(Delivery.DeliveryComment)s,
        %(Department)s,
        %(Delivery.Region)s,
        %(Delivery.Number)s,
        %(Delivery.CustomerName)s,
        %(Delivery.Phone)s,
        %(Delivery.Address)s,
        %(Delivery.Courier)s,
        now()
    )
    ON CONFLICT (department, delivery_cooking_finish_time, delivery_number)
    DO UPDATE SET
        delivery_print_time = EXCLUDED.delivery_print_time,
        delivery_send_time = EXCLUDED.delivery_send_time,
        delivery_actual_time = EXCLUDED.delivery_actual_time,
        delivery_close_time = EXCLUDED.delivery_close_time,
        delivery_expected_time = EXCLUDED.delivery_expected_time,
        delivery_source_key = EXCLUDED.delivery_source_key,
        delivery_comment = EXCLUDED.delivery_comment,
        delivery_customer_name = EXCLUDED.delivery_customer_name,
        delivery_phone = EXCLUDED.delivery_phone,
        delivery_address = EXCLUDED.delivery_address,
        delivery_courier = EXCLUDED.delivery_courier,
        updated_at = now();
    """

    for row in rows:
        cur.execute(query, row)

    conn.commit()
    cur.close()
    conn.close()

    print("✅ Данные успешно записаны!")


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
