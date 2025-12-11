import os
import datetime as dt
import requests
import psycopg2
from dotenv import load_dotenv

load_dotenv()

# --- Настройки iiko ---
IIKO_BASE_URL = os.getenv("IIKO_BASE_URL", "").rstrip("/")
IIKO_LOGIN = os.getenv("IIKO_LOGIN")
IIKO_PASSWORD = os.getenv("IIKO_PASSWORD")

# --- Настройки Postgres (Neon) ---
def get_pg_connection():
    return psycopg2.connect(
        host=os.getenv("PG_HOST"),
        port=os.getenv("PG_PORT"),
        dbname=os.getenv("PG_DB"),
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD"),
        sslmode=os.getenv("PG_SSLMODE", "require"),
    )

# --- Токен iiko (как в etl_iiko_t1_light.py) ---
def get_token():
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

# --- Период выгрузки: вчера по умолчанию ---
def get_period():
    date_from_str = os.getenv("DATE_FROM")
    date_to_str = os.getenv("DATE_TO")

    if date_from_str and date_to_str:
        date_from = dt.date.fromisoformat(date_from_str)
        date_to = dt.date.fromisoformat(date_to_str)
        print(f"📅 Используем период из ENV: {date_from} – {date_to}")
        return date_from, date_to

    today = dt.date.today()
    date_from = today - dt.timedelta(days=1)
    date_to = today
    print(f"📅 Используем период по умолчанию: {date_from} – {date_to}")
    return date_from, date_to

# --- Универсальная функция для OLAP ---
def fetch_margin(token, date_from, date_to, courier_only: bool):
    label = "КУРЬЕР" if courier_only else "ВСЕ"
    print(f"📦 Загружаем данные 'Маржа ДМД' ({label}) из iiko...")

    url = f"{IIKO_BASE_URL}/api/v2/reports/olap"
    params = {"key": token}

    filters = {
        "SessionID.OperDay": {
            "filterType": "DateRange",
            "periodType": "CUSTOM",
            "from": date_from.strftime("%Y-%m-%d"),
            "to": date_to.strftime("%Y-%m-%d"),
            "includeLow": True,
            "includeHigh": False,
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
    }

    if courier_only:
        filters["Delivery.ServiceType"] = {
            "filterType": "IncludeValues",
            "values": ["COURIER"],
        }

    body = {
        "reportType": "SALES",
        "buildSummary": False,
        "groupByRowFields": [
            "Department",
            "OpenDate.Typed",
        ],
        "aggregateFields": [
            "DishSumInt",
            "DiscountSum",
            "ProductCostBase.ProductCost",
        ],
        "filters": filters,
    }

    resp = requests.post(url, params=params, json=body, timeout=90)

    print(f"HTTP статус iiko ({label}):", resp.status_code)
    print("Тело ответа (первые 500 символов):")
    print(resp.text[:500])

    resp.raise_for_status()
    data = resp.json().get("data", [])
    print(f"✅ Получено строк ({label}): {len(data)}")
    return data

# --- Запись в margin_iiko ---
def upsert_margin(rows_all, rows_courier):
    print("💾 Обновляем таблицу margin_iiko...")

    # словарь по (department, oper_day) для курьерских строк
    courier_map = {}
    for r in rows_courier:
        dep = r.get("Department")
        oper_raw = r.get("OpenDate.Typed")  # ожидаем '2025-12-07T00:00:00' или '2025-12-07'
        if not dep or not oper_raw:
            continue
        oper_day = oper_raw[:10]
        key = (dep, oper_day)
        courier_map[key] = {
            "revenue_courier": float(r.get("DishSumInt") or 0),
            "discount_courier": float(r.get("DiscountSum") or 0),
            "product_cost_courier": float(r.get("ProductCostBase.ProductCost") or 0),
        }

    conn = get_pg_connection()
    cur = conn.cursor()

    query = """
    INSERT INTO margin_iiko (
        department,
        oper_day,
        revenue,
        discount,
        product_cost,
        revenue_courier,
        discount_courier,
        product_cost_courier,
        updated_at
    )
    VALUES (
        %(department)s,
        %(oper_day)s,
        %(revenue)s,
        %(discount)s,
        %(product_cost)s,
        %(revenue_courier)s,
        %(discount_courier)s,
        %(product_cost_courier)s,
        now()
    )
    ON CONFLICT (department, oper_day)
    DO UPDATE SET
        revenue = EXCLUDED.revenue,
        discount = EXCLUDED.discount,
        product_cost = EXCLUDED.product_cost,
        revenue_courier = EXCLUDED.revenue_courier,
        discount_courier = EXCLUDED.discount_courier,
        product_cost_courier = EXCLUDED.product_cost_courier,
        updated_at = now();
    """

    rows_to_upsert = 0

    for r in rows_all:
        dep = r.get("Department")
        oper_raw = r.get("OpenDate.Typed")
        if not dep or not oper_raw:
            continue

        oper_day = oper_raw[:10]

        key = (dep, oper_day)
        courier_vals = courier_map.get(
            key,
            {
                "revenue_courier": 0.0,
                "discount_courier": 0.0,
                "product_cost_courier": 0.0,
            },
        )

        payload = {
            "department": dep,
            "oper_day": oper_day,
            "revenue": float(r.get("DishSumInt") or 0),
            "discount": float(r.get("DiscountSum") or 0),
            "product_cost": float(r.get("ProductCostBase.ProductCost") or 0),
            **courier_vals,
        }

        cur.execute(query, payload)
        rows_to_upsert += 1

    conn.commit()
    cur.close()
    conn.close()

    print(f"✅ В margin_iiko upsert'нуто строк: {rows_to_upsert}")

# --- Основной процесс ---
def main():
    date_from, date_to = get_period()
    print(f"🚀 ETL MARGIN DAILY: {date_from} – {date_to}")

    token = get_token()
    try:
        rows_all = fetch_margin(token, date_from, date_to, courier_only=False)
        rows_courier = fetch_margin(token, date_from, date_to, courier_only=True)
        upsert_margin(rows_all, rows_courier)
    finally:
        logout(token)
        print("🔐 Logout выполнен")

if __name__ == "__main__":
    main()
