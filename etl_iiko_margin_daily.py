import os
import datetime as dt
import requests
import psycopg2
from dotenv import load_dotenv

# Загружаем .env (локально) / переменные окружения (в GitHub)
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


# --- Токен iiko ---
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
    date_to = today  # правая граница, в iiko будет includeHigh=False
    print(f"📅 Используем период по умолчанию: {date_from} – {date_to}")
    return date_from, date_to


# --- Универсальная функция для OLAP ---
# delivery_type:
#   "ALL"     – без фильтра по Delivery.ServiceType
#   "COURIER" – Delivery.ServiceType = COURIER
#   "PICKUP"  – Delivery.ServiceType = PICKUP
def fetch_margin(token, date_from, date_to, delivery_type: str):
    label = {
        "ALL": "ВСЕ",
        "COURIER": "КУРЬЕР",
        "PICKUP": "САМОВЫВОЗ",
    }[delivery_type]

    print(f"🚚 Загружаем данные 'Маржа ДМД' ({label}) из iiko...")

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

    # Фильтр по типу доставки
    if delivery_type in ("COURIER", "PICKUP"):
        filters["Delivery.ServiceType"] = {
            "filterType": "IncludeValues",
            "values": ["COURIER" if delivery_type == "COURIER" else "PICKUP"],
        }

    body = {
        "reportType": "SALES",
        "groupByRowFields": ["Department", "OpenDate.Typed"],
        "aggregateFields": [
            "DishSumInt",
            "DiscountSum",
            "ProductCostBase.ProductCost",
        ],
        "filters": filters,
    }

    resp = requests.post(url, json=body, params=params, timeout=90)
    resp.raise_for_status()
    data = resp.json()

    rows = []
    for r in data.get("data", []):
        dep = r.get("Department")
        oper_raw = r.get("OpenDate.Typed")
        if not dep or not oper_raw:
            continue

        oper_day = oper_raw[:10]  # 'YYYY-MM-DD'
        rows.append(
            {
                "department": dep,
                "oper_day": oper_day,
                "revenue": float(r.get("DishSumInt") or 0),
                "discount": float(r.get("DiscountSum") or 0),
                "product_cost": float(r.get("ProductCostBase.ProductCost") or 0),
            }
        )

    print(f"📊 Получено строк ({label}): {len(rows)}")
    return rows


# --- Запись базовых значений (ALL) ---
def upsert_base_margin(conn, rows):
    if not rows:
        print("⚠️ Нет строк для записи (ALL)")
        return

    cur = conn.cursor()
    sql = """
        INSERT INTO margin_iiko (
            department,
            oper_day,
            revenue,
            discount,
            product_cost,
            updated_at
        )
        VALUES (%s, %s, %s, %s, %s, now())
        ON CONFLICT (department, oper_day)
        DO UPDATE SET
            revenue = EXCLUDED.revenue,
            discount = EXCLUDED.discount,
            product_cost = EXCLUDED.product_cost,
            updated_at = now();
    """

    for r in rows:
        cur.execute(
            sql,
            (
                r["department"],
                r["oper_day"],
                r["revenue"],
                r["discount"],
                r["product_cost"],
            ),
        )

    conn.commit()
    cur.close()
    print(f"✅ В margin_iiko записано (ALL): {len(rows)} строк")


# --- Запись по типу доставки (COURIER / PICKUP) ---
def upsert_type_margin(conn, rows, delivery_type: str):
    if not rows:
        print(f"⚠️ Нет строк для записи ({delivery_type})")
        return

    if delivery_type == "COURIER":
        revenue_field = "revenue_courier"
        discount_field = "discount_courier"
        cost_field = "product_cost_courier"
    elif delivery_type == "PICKUP":
        revenue_field = "revenue_pickup"
        discount_field = "discount_pickup"
        cost_field = "product_cost_pickup"
    else:
        raise ValueError(f"Unknown delivery_type: {delivery_type}")

    cur = conn.cursor()

    # ВАЖНО: заполняем базовые поля нулями, чтобы не нарушать NOT NULL,
    # а при конфликте обновляем только поля конкретного типа доставки.
    sql = f"""
        INSERT INTO margin_iiko (
            department,
            oper_day,
            revenue,
            discount,
            product_cost,
            {revenue_field},
            {discount_field},
            {cost_field},
            updated_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, now())
        ON CONFLICT (department, oper_day)
        DO UPDATE SET
            {revenue_field} = EXCLUDED.{revenue_field},
            {discount_field} = EXCLUDED.{discount_field},
            {cost_field} = EXCLUDED.{cost_field},
            updated_at = now();
    """

    for r in rows:
        cur.execute(
            sql,
            (
                r["department"],
                r["oper_day"],
                0.0,  # revenue (общая) — 0, если строки ALL не было
                0.0,  # discount (общая)
                0.0,  # product_cost (общая)
                r["revenue"],
                r["discount"],
                r["product_cost"],
            ),
        )

    conn.commit()
    cur.close()
    print(f"✅ В margin_iiko записано ({delivery_type}): {len(rows)} строк")


# --- Основной процесс ---
def main():
    date_from, date_to = get_period()
    print(f"🚀 ETL MARGIN DAILY: {date_from} – {date_to}")

    token = get_token()
    try:
        # 1) Все заказы
        rows_all = fetch_margin(token, date_from, date_to, "ALL")

        # 2) Только курьер
        rows_courier = fetch_margin(token, date_from, date_to, "COURIER")

        # 3) Только самовывоз
        rows_pickup = fetch_margin(token, date_from, date_to, "PICKUP")

        # --- Запись в Postgres ---
        conn = get_pg_connection()
        try:
            upsert_base_margin(conn, rows_all)
            upsert_type_margin(conn, rows_courier, "COURIER")
            upsert_type_margin(conn, rows_pickup, "PICKUP")
        finally:
            conn.close()
            print("🔌 Соединение с Postgres закрыто")
    finally:
        logout(token)
        print("🔐 Logout выполнен")


if __name__ == "__main__":
    main()
