import os
import datetime as dt
import requests
import psycopg2
from dotenv import load_dotenv

load_dotenv()

IIKO_BASE_URL = os.getenv("IIKO_BASE_URL", "").rstrip("/")
IIKO_LOGIN = os.getenv("IIKO_LOGIN")
IIKO_PASSWORD = os.getenv("IIKO_PASSWORD")


def get_pg_connection():
    return psycopg2.connect(
        host=os.getenv("PG_HOST"),
        port=os.getenv("PG_PORT"),
        dbname=os.getenv("PG_DB"),
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD"),
        sslmode=os.getenv("PG_SSLMODE", "require")
    )


def upsert_sales_daily(data):
    print("📦 Записываем данные в базу...")

    conn = get_pg_connection()
    cursor = conn.cursor()

    query = """
    INSERT INTO iiko_sales_daily (
        open_date,
        dish_amount,
        dish_discount_sum,
        dish_sum,
        updated_at
    )
    VALUES (%s, %s, %s, %s, now())
    ON CONFLICT (open_date)
    DO UPDATE SET
        dish_amount = EXCLUDED.dish_amount,
        dish_discount_sum = EXCLUDED.dish_discount_sum,
        dish_sum = EXCLUDED.dish_sum,
        updated_at = now();
    """

    for row in data["data"]:
        cursor.execute(query, (
            row["OpenDate.Typed"],
            row["DishAmountInt"],
            row["DishDiscountSumInt"],
            row["DishSumInt"]
        ))

    conn.commit()
    cursor.close()
    conn.close()

    print("✅ Данные записаны в базу!")


def get_token():
    url = f"{IIKO_BASE_URL}/api/auth"
    params = {"login": IIKO_LOGIN, "pass": IIKO_PASSWORD}

    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()

    token = resp.text.strip()
    print(f"✅ Токен получен: {token[:6]}...")
    return token


def logout(token: str):
    url = f"{IIKO_BASE_URL}/api/logout"
    params = {"key": token}
    requests.post(url, params=params, timeout=10)


def fetch_sales_for_period(token, date_from, date_to):
    url = f"{IIKO_BASE_URL}/api/v2/reports/olap"

    body = {
        "reportType": "SALES",
        "buildSummary": False,
        "groupByRowFields": ["OpenDate.Typed"],
        "aggregateFields": ["DishAmountInt", "DishDiscountSumInt", "DishSumInt"],
        "filters": {
            "OpenDate.Typed": {
                "filterType": "DateRange",
                "periodType": "CUSTOM",
                "from": str(date_from),
                "to": str(date_to),
                "includeLow": True,
                "includeHigh": True
            }
        }
    }

    params = {"key": token}

    resp = requests.post(url, params=params, json=body, timeout=60)
    resp.raise_for_status()
    return resp.json()


def main():
    today = dt.date.today()
    date_to = today - dt.timedelta(days=1)
    date_from = today - dt.timedelta(days=7)

    print(f"🚀 Старт ETL. Период: {date_from} – {date_to}")

    token = get_token()
    try:
        data = fetch_sales_for_period(token, date_from, date_to)
        print("✅ Данные получены от iiko")

        upsert_sales_daily(data)

    finally:
        logout(token)
        print("🔐 Logout выполнен.")


if __name__ == "__main__":
    main()