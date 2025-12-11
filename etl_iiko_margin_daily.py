import os
import datetime as dt
import requests
import psycopg2
from dotenv import load_dotenv

# Загружаем .env при локальном запуске
load_dotenv()

# Настройки iiko
IIKO_BASE_URL = os.getenv("IIKO_BASE_URL", "").rstrip("/")
IIKO_LOGIN = os.getenv("IIKO_LOGIN")
IIKO_PASSWORD = os.getenv("IIKO_PASSWORD")

# Подключение к Postgres (Neon)
def get_pg():
    return psycopg2.connect(
        host=os.getenv("PG_HOST"),
        port=os.getenv("PG_PORT"),
        dbname=os.getenv("PG_DB"),
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD"),
        sslmode=os.getenv("PG_SSLMODE", "require"),
    )

# Получение токена
def get_token():
    url = f"{IIKO_BASE_URL}/api/auth"
    resp = requests.get(url, params={"login": IIKO_LOGIN, "pass": IIKO_PASSWORD})
    resp.raise_for_status()
    token = resp.text.strip()
    print(f"🔑 Token: {token[:6]}...")
    return token

def logout(token):
    try:
        requests.post(f"{IIKO_BASE_URL}/api/logout", params={"key": token})
    except:
        pass

# Период
def get_period():
    f = os.getenv("DATE_FROM")
    t = os.getenv("DATE_TO")

    if f and t:
        d1 = dt.date.fromisoformat(f)
        d2 = dt.date.fromisoformat(t)
        if d2 <= d1:
            d2 = d1 + dt.timedelta(days=1)
        return d1, d2

    # default — вчера
    y = dt.date.today() - dt.timedelta(days=1)
    return y, y + dt.timedelta(days=1)

# Запрос к iiko OLAP
def fetch_daily_margin(token, d1, d2):
    print("📦 Fetching daily margin data...")

    url = f"{IIKO_BASE_URL}/api/v2/reports/olap"
    params = {"key": token}

    body = {
        "reportType": "SALES",
        "buildSummary": False,
        "groupByRowFields": [
            "Department",
            "OpenDate.Typed"      # Учетный день
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
                "from": d1.strftime("%Y-%m-%d"),
                "to": d2.strftime("%Y-%m-%d"),
                "includeLow": True,
                "includeHigh": True
            },
            "Storned": {
                "filterType": "IncludeValues",
                "values": ["FALSE"]
            },
            "DeletedWithWriteoff": {
                "filterType": "IncludeValues",
                "values": ["NOT_DELETED"]
            },
            "OrderDeleted": {
                "filterType": "IncludeValues",
                "values": ["NOT_DELETED"]
            },
            "Department": {
                "filterType": "IncludeValues",
                "values": ["Авиагородок", "Домодедово"]
            }
        }
    }

    r = requests.post(url, params=params, json=body, timeout=90)
    print("HTTP:", r.status_code)
    print("Ответ (первые 500 знаков):", r.text[:500])
    r.raise_for_status()

    return r.json().get("data", [])

# Загрузка в Neon
def save_to_db(rows):
    if not rows:
        print("⚠ Нет данных для записи")
        return

    conn = get_pg()
    cur = conn.cursor()

    query = """
    INSERT INTO margin_iiko (
        department,
        oper_day,
        revenue,
        discount,
        product_cost,
        updated_at
    )
    VALUES (
        %(Department)s,
        %(OpenDate.Typed)s,
        %(DishSumInt)s,
        %(DiscountSum)s,
        %(ProductCostBase.ProductCost)s,
        now()
    )
    ON CONFLICT (department, oper_day)
    DO UPDATE SET
        revenue      = EXCLUDED.revenue,
        discount     = EXCLUDED.discount,
        product_cost = EXCLUDED.product_cost,
        updated_at   = now();
    """

    for r in rows:
        cur.execute(query, r)

    conn.commit()
    cur.close()
    conn.close()
    print(f"💾 Saved {len(rows)} rows into margin_iiko")

# Основной процесс
def main():
    d1, d2 = get_period()
    print(f"🚀 ETL DAILY MARGIN: {d1} → {d2}")

    token = get_token()

    try:
        rows = fetch_daily_margin(token, d1, d2)
        print(f"📊 Rows: {len(rows)}")
        save_to_db(rows)
    finally:
        logout(token)
        print("🔐 Logout OK")

if __name__ == "__main__":
    main()
