import os
import datetime as dt
import requests
import psycopg2
from dotenv import load_dotenv

# Загружаем переменные окружения при локальном запуске
load_dotenv()

# IIKO
IIKO_BASE_URL = os.getenv("IIKO_BASE_URL", "").rstrip("/")
IIKO_LOGIN = os.getenv("IIKO_LOGIN")
IIKO_PASSWORD = os.getenv("IIKO_PASSWORD")

# PostgreSQL (Neon)
def get_pg_connection():
    return psycopg2.connect(
        host=os.getenv("PG_HOST"),
        port=os.getenv("PG_PORT"),
        dbname=os.getenv("PG_DB"),
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD"),
        sslmode=os.getenv("PG_SSLMODE", "require"),
    )

def get_token():
    url = f"{IIKO_BASE_URL}/api/auth"
    resp = requests.get(url, params={"login": IIKO_LOGIN, "pass": IIKO_PASSWORD}, timeout=30)
    resp.raise_for_status()
    token = resp.text.strip()
    print(f"🔑 Token: {token[:6]}...")
    return token

def logout(token):
    try:
        requests.post(f"{IIKO_BASE_URL}/api/logout", params={"key": token}, timeout=10)
    except Exception:
        pass

def get_period():
    f = os.getenv("DATE_FROM")
    t = os.getenv("DATE_TO")

    if f and t:
        date_from = dt.date.fromisoformat(f)
        date_to = dt.date.fromisoformat(t)
        if date_to <= date_from:
            date_to = date_from + dt.timedelta(days=1)
        print(f"📅 Period from ENV: {date_from} → {date_to}")
        return date_from, date_to

    yesterday = dt.date.today() - dt.timedelta(days=1)
    date_from = yesterday
    date_to = yesterday + dt.timedelta(days=1)
    print(f"📅 Default period: {date_from} → {date_to}")
    return date_from, date_to

def fetch_data(token, date_from, date_to):
    print("📦 Fetching Margin DMD data...")

    url = f"{IIKO_BASE_URL}/api/v2/reports/olap"
    params = {"key": token}
    body = {
        "reportType": "SALES",
        "buildSummary": False,
        "groupByRowFields": [
            "CloseTime",
            "OpenTime",
            "OpenDate.Typed",        # 👈 учётный день
            "Department",
            "Delivery.SourceKey",
            "OrderType",
            "Delivery.Region",
        ],
        "aggregateFields": [
            "DishSumInt",
            "DiscountSum",
            "ProductCostBase.ProductCost",
        ],
        "filters": {
            "OpenDate.Typed": {
                "filterType": "DateRange",
                "periodType": "CUSTOM",
                "from": date_from.strftime("%Y-%m-%d"),
                "to": date_to.strftime("%Y-%m-%d"),
                "includeLow": True,
                "includeHigh": True,
            },
            "Storned": {
                "filterType": "IncludeValues",
                "values": ["FALSE"],
            },
            "DeletedWithWriteoff": {
                "filterType": "IncludeValues",
                "values": ["NOT_DELETED"],
            },
            "Department": {
                "filterType": "IncludeValues",
                "values": ["Авиагородок", "Домодедово"],
            },
            "OrderDeleted": {
                "filterType": "IncludeValues",
                "values": ["NOT_DELETED"],
            },
        },
    }

    r = requests.post(url, params=params, json=body, timeout=120)
    print("HTTP:", r.status_code)
    r.raise_for_status()

    data = r.json().get("data", [])
    return data

def save_to_db(rows):
    if not rows:
        print("⚠️ No rows.")
        return

    conn = get_pg_connection()
    cur = conn.cursor()

    query = """
    INSERT INTO iiko_margin_dmd (
        department,
        close_time,
        open_time,
        open_date,
        delivery_source_key,
        order_type,
        delivery_region,
        dish_sum_int,
        discount_sum,
        product_cost,
        updated_at
    )
    VALUES (
        %(Department)s,
        %(CloseTime)s,
        %(OpenTime)s,
        %(OpenDate.Typed)s,
        %(Delivery.SourceKey)s,
        %(OrderType)s,
        %(Delivery.Region)s,
        %(DishSumInt)s,
        %(DiscountSum)s,
        %(ProductCostBase.ProductCost)s,
        now()
    )
    ON CONFLICT (department, close_time, open_time, delivery_source_key, order_type, delivery_region)
    DO UPDATE SET
        open_date     = EXCLUDED.open_date,
        dish_sum_int  = EXCLUDED.dish_sum_int,
        discount_sum  = EXCLUDED.discount_sum,
        product_cost  = EXCLUDED.product_cost,
        updated_at    = now();
    """

    for r in rows:
        # Нормализация полей, которые нам нужны NOT NULL в ключе
        if r.get("Delivery.Region") in (None, "", "null"):
            r["Delivery.Region"] = "Без зоны"

        if r.get("Delivery.SourceKey") in (None, "", "null"):
            r["Delivery.SourceKey"] = "Не указан"

        if r.get("OrderType") in (None, "", "null"):
            r["OrderType"] = "Не указан"

        cur.execute(query, r)

    conn.commit()
    cur.close()
    conn.close()

    print(f"💾 Saved {len(rows)} rows to Neon.")

def main():
    date_from, date_to = get_period()
    print(f"🚀 Margin DMD ETL: {date_from} → {date_to}")
    token = get_token()

    try:
        rows = fetch_data(token, date_from, date_to)
        print(f"📊 Rows: {len(rows)}")
        save_to_db(rows)
    finally:
        logout(token)
        print("🔐 Logout")

if __name__ == "__main__":
    main()
