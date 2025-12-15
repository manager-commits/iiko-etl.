import os
import datetime as dt
import requests
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()

# --- iiko ---
IIKO_BASE_URL = os.getenv("IIKO_BASE_URL", "").rstrip("/")
IIKO_LOGIN = os.getenv("IIKO_LOGIN")
IIKO_PASSWORD = os.getenv("IIKO_PASSWORD")

# --- Postgres (Neon) ---
def get_pg_connection():
    return psycopg2.connect(
        host=os.getenv("PG_HOST"),
        port=os.getenv("PG_PORT"),
        dbname=os.getenv("PG_DB"),
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD"),
        sslmode=os.getenv("PG_SSLMODE", "require"),
    )

# --- Token ---
def get_token() -> str:
    url = f"{IIKO_BASE_URL}/api/auth"
    params = {"login": IIKO_LOGIN, "pass": IIKO_PASSWORD}
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    token = resp.text.strip()
    print(f"🔑 Токен получен: {token[:6]}...")
    return token

def logout(token: str):
    url = f"{IIKO_BASE_URL}/api/logout"
    params = {"key": token}
    try:
        requests.post(url, params=params, timeout=10)
    except Exception as e:
        print("⚠️ Ошибка при logout:", e)

# --- Period (DATE_FROM/DATE_TO or yesterday->today) ---
def get_period():
    date_from_str = os.getenv("DATE_FROM")
    date_to_str = os.getenv("DATE_TO")

    if date_from_str and date_to_str:
        date_from = dt.date.fromisoformat(date_from_str)
        date_to = dt.date.fromisoformat(date_to_str)
        print(f"📅 Период из ENV: {date_from} – {date_to}")
        return date_from, date_to

    today = dt.date.today()
    date_from = today - dt.timedelta(days=1)
    date_to = today
    print(f"📅 Период по умолчанию (вчера): {date_from} – {date_to}")
    return date_from, date_to

# --- OLAP fetch ---
def fetch_discount_types(token: str, date_from: dt.date, date_to: dt.date):
    print("📊 Загружаем OLAP 'Типы скидок' из iiko...")

    url = f"{IIKO_BASE_URL}/api/v2/reports/olap"
    params = {"key": token}

    filters = {
        "OpenDate.Typed": {
            "filterType": "DateRange",
            "periodType": "CUSTOM",
            "from": date_from.strftime("%Y-%m-%d"),
            "to": date_to.strftime("%Y-%m-%d"),
            "includeLow": True,
            "includeHigh": False,
        },
        "DeletedWithWriteoff": {
            "filterType": "IncludeValues",
            "values": ["NOT_DELETED"],
        },
        "OrderDeleted": {
            "filterType": "IncludeValues",
            "values": ["NOT_DELETED"],
        },
        "Storned": {
            "filterType": "IncludeValues",
            "values": ["FALSE"],
        },
        # как ты прислал (значения именно такие)
        "OrderType": {
            "filterType": "IncludeValues",
            "values": ["Delivery by courier", "Доставка самовывоз"],
        },
    }

    body = {
        "reportType": "SALES",
        "groupByRowFields": [
            "OpenDate.Typed",
            "Department",
            "OrderDiscount.Type",
        ],
        "aggregateFields": [
            "DishSumInt",
            "DiscountSum",
            "UniqOrderId.OrdersCount",
        ],
        "filters": filters,
    }

    resp = requests.post(url, json=body, params=params, timeout=120)
    resp.raise_for_status()
    data = resp.json()

    rows = []
    for r in data.get("data", []):
        dep = r.get("Department")
        oper = r.get("OpenDate.Typed")
        disc_type = r.get("OrderDiscount.Type")

        if not dep or not oper:
            continue

        # iiko обычно отдаёт дату строкой; берём YYYY-MM-DD
        oper_day = oper[:10] if isinstance(oper, str) else oper

        disc_type = (disc_type or "").strip()
        if not disc_type:
            disc_type = "Без скидки"

        rows.append({
            "department": dep,
            "oper_day": oper_day,
            "discount_type": disc_type,
            "orders_count": int(float(r.get("UniqOrderId.OrdersCount") or 0)),
            "revenue": float(r.get("DishSumInt") or 0),
            "discount_sum": float(r.get("DiscountSum") or 0),
        })

    print(f"✅ Получено строк: {len(rows)}")
    return rows

# --- Upsert ---
def upsert_discount_types(conn, rows):
    if not rows:
        print("⚠️ Нет данных для записи")
        return

    sql = """
        INSERT INTO discount_types_daily_iiko (
            department,
            oper_day,
            discount_type,
            orders_count,
            revenue,
            discount_sum,
            updated_at
        )
        VALUES %s
        ON CONFLICT (department, oper_day, discount_type)
        DO UPDATE SET
            orders_count = EXCLUDED.orders_count,
            revenue      = EXCLUDED.revenue,
            discount_sum = EXCLUDED.discount_sum,
            updated_at   = now();
    """

    values = [
        (
            r["department"],
            r["oper_day"],
            r["discount_type"],
            r["orders_count"],
            r["revenue"],
            r["discount_sum"],
        )
        for r in rows
    ]

    with conn.cursor() as cur:
        execute_values(cur, sql, values, page_size=500)
    conn.commit()
    print(f"💾 Upsert'нуто строк: {len(rows)}")

def main():
    date_from, date_to = get_period()
    print(f"🚀 ETL DISCOUNT TYPES: {date_from} – {date_to}")

    token = get_token()
    try:
        rows = fetch_discount_types(token, date_from, date_to)
        conn = get_pg_connection()
        try:
            upsert_discount_types(conn, rows)
        finally:
            conn.close()
            print("🔌 Соединение с Postgres закрыто")
    finally:
        logout(token)
        print("🔐 Logout выполнен")

if __name__ == "__main__":
    main()
