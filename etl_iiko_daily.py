import os
import datetime as dt
import requests
import psycopg2
from dotenv import load_dotenv

# Загружаем переменные окружения (локально; в GitHub Actions secrets передаются напрямую)
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
        sslmode=os.getenv("PG_SSLMODE", "require"),
    )


def upsert_sales_daily(data: dict) -> None:
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

    for row in data.get("data", []):
        cursor.execute(
            query,
            (
                row["OpenDate.Typed"],
                row["DishAmountInt"],
                row["DishDiscountSumInt"],
                row["DishSumInt"],
            ),
        )

    conn.commit()
    cursor.close()
    conn.close()

    print("✅ Данные записаны в базу!")


def get_token() -> str:
    """Авторизация в iiko, возвращает токен."""
    url = f"{IIKO_BASE_URL}/api/auth"
    params = {"login": IIKO_LOGIN, "pass": IIKO_PASSWORD}

    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()

    token = resp.text.strip()
    print(f"✅ Токен получен: {token[:6]}...")
    return token


def logout(token: str) -> None:
    """Корректный выход из iiko."""
    url = f"{IIKO_BASE_URL}/api/logout"
    params = {"key": token}
    try:
        requests.post(url, params=params, timeout=10)
    except Exception as e:
        print("⚠️ Ошибка при logout:", e)


def fetch_sales_for_period(token: str, date_from: dt.date, date_to: dt.date) -> dict:
    """
    Запрос OLAP-отчёта SALES по дням.
    groupByRowFields = OpenDate.Typed
    агрегаты: количество блюд, сумма скидки, сумма продаж.
    """
    url = f"{IIKO_BASE_URL}/api/v2/reports/olap"

    body = {
        "reportType": "SALES",
        "buildSummary": False,
        "groupByRowFields": ["OpenDate.Typed"],
        "groupByColFields": [],
        "aggregateFields": [
            "DishAmountInt",
            "DishDiscountSumInt",
            "DishSumInt",
        ],
        "filters": {
            "OpenDate.Typed": {
                "filterType": "DateRange",
                "periodType": "CUSTOM",
                "from": date_from.strftime("%Y-%m-%d"),
                "to": date_to.strftime("%Y-%m-%d"),
                "includeLow": True,
                "includeHigh": True,
            }
        },
    }

    params = {"key": token}

    print(f"Делаем OLAP-запрос SALES за период {date_from} – {date_to}...")
    resp = requests.post(url, params=params, json=body, timeout=60)
    resp.raise_for_status()

    data = resp.json()
    return data


def calculate_period():
    """
    Выбираем период в зависимости от режима:
    - NOVEMBER_FULL: весь ноябрь текущего года
    - DAILY (по умолчанию): только вчерашний день
    """
    mode = os.getenv("ETL_MODE", "DAILY").upper()
    today = dt.date.today()

    if mode == "NOVEMBER_FULL":
        year = today.year
        date_from = dt.date(year, 11, 1)
        date_to = dt.date(year, 11, 30)

        # На всякий случай не лезем в будущее
        max_to = today - dt.timedelta(days=1)
        if date_to > max_to:
            date_to = max_to
    else:
        # режим по умолчанию: только вчера
        date_to = today - dt.timedelta(days=1)
        date_from = date_to

    return date_from, date_to, mode


def main():
    date_from, date_to, mode = calculate_period()

    print(f"🚀 Старт ETL (режим: {mode}). Период: {date_from} – {date_to}")

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
