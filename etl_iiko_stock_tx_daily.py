import os
import datetime as dt
import requests
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

# Загружаем .env (локально) / переменные окружения (в GitHub)
load_dotenv()

# --- Настройки iiko ---
IIKO_BASE_URL = os.getenv("IIKO_BASE_URL", "").rstrip("/")
IIKO_LOGIN = os.getenv("IIKO_LOGIN")
IIKO_PASSWORD = os.getenv("IIKO_PASSWORD")

# Фильтры как в отчёте
DEPARTMENTS = ["Авиагородок", "Домодедово"]
PRODUCT_NUM_FILTER = ["00001"]  # как в твоих параметрах отчёта

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
    if not IIKO_BASE_URL:
        raise RuntimeError("IIKO_BASE_URL is not set")
    if not IIKO_LOGIN or not IIKO_PASSWORD:
        raise RuntimeError("IIKO_LOGIN / IIKO_PASSWORD is not set")

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
        print("🔐 Logout выполнен")
    except Exception as e:
        print("⚠️ Ошибка при logout:", e)

# --- Период выгрузки: вчера по умолчанию ---
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
    date_to = today  # правая граница, includeHigh=False
    print(f"📅 Период по умолчанию (вчера): {date_from} – {date_to}")
    return date_from, date_to

# --- OLAP: "Отчет по проводкам" ---
def fetch_stock_tx(token: str, date_from: dt.date, date_to: dt.date):
    print("📦 Загружаем OLAP 'Отчет по проводкам' из iiko...")

    url = f"{IIKO_BASE_URL}/api/v2/reports/olap"
    params = {"key": token}

    filters = {
        "DateTime.OperDayFilter": {
            "filterType": "DateRange",
            "periodType": "CUSTOM",
            "from": date_from.strftime("%Y-%m-%d"),
            "to": date_to.strftime("%Y-%m-%d"),
            "includeLow": True,
            "includeHigh": False,
        },
        "Product.Num": {
            "filterType": "IncludeValues",
            "values": PRODUCT_NUM_FILTER,
        },
        "Department": {
            "filterType": "IncludeValues",
            "values": DEPARTMENTS,
        },
    }

    body = {
        "reportType": "TRANSACTIONS",
        "groupByRowFields": [
            "DateTime.DateTyped",
            "Product.Num",
            "Product.Name",
            "Department",
            "Product.Type",
            "Product.MeasureUnit",
            "Document",
            "TransactionType",
        ],
        "aggregateFields": [
            "Amount.StoreInOutTyped",
        ],
        "filters": filters,
    }

    resp = requests.post(url, json=body, params=params, timeout=90)
    resp.raise_for_status()
    data = resp.json()

    rows = []
    for r in data.get("data", []):
        dep = r.get("Department")
        oper_raw = r.get("DateTime.DateTyped")
        if not dep or not oper_raw:
            continue

        oper_day = oper_raw[:10] if isinstance(oper_raw, str) else oper_raw

        rows.append(
            {
                "department": dep,
                "oper_day": oper_day,
                "product_num": r.get("Product.Num"),
                "product_name": r.get("Product.Name"),
                "product_type": r.get("Product.Type"),
                "measure_unit": r.get("Product.MeasureUnit"),
                "document": r.get("Document"),
                "transaction_type": r.get("TransactionType"),
                "turnover": float(r.get("Amount.StoreInOutTyped") or 0),
            }
        )

    print(f"✅ Получено строк из iiko: {len(rows)}")
    print("🔎 Первые 10 строк из iiko:")
    for i, x in enumerate(rows[:10], start=1):
        print(f"{i:02d}. {x}")

    return rows

def aggregate_without_document(rows):
    """
    ВАЖНО:
    Твой отчёт в iiko группируется по Document, но в таблице stock_tx_iiko
    столбца document НЕТ (по ошибке в логах).
    Чтобы не терять оборот, суммируем turnover по ключу без document.
    """
    agg = {}
    for r in rows:
        key = (
            r["department"],
            r["oper_day"],
            r["product_num"],
            r.get("product_name"),
            r.get("product_type"),
            r.get("measure_unit"),
            r.get("transaction_type"),
        )
        if key not in agg:
            agg[key] = {
                "department": r["department"],
                "oper_day": r["oper_day"],
                "product_num": r["product_num"],
                "product_name": r.get("product_name"),
                "product_type": r.get("product_type"),
                "measure_unit": r.get("measure_unit"),
                "transaction_type": r.get("transaction_type"),
                "turnover": 0.0,
            }
        agg[key]["turnover"] += float(r.get("turnover") or 0.0)

    return list(agg.values())

def upsert_stock_tx(conn, rows):
    if not rows:
        print("⚠️ Нет строк для записи в БД")
        return 0

    # ВНИМАНИЕ: document здесь НЕТ — потому что в таблице его нет
    sql = """
        INSERT INTO stock_tx_iiko (
            department,
            oper_day,
            product_num,
            product_name,
            product_type,
            measure_unit,
            transaction_type,
            turnover,
            updated_at
        )
        VALUES %s
        ON CONFLICT (department, oper_day, product_num, transaction_type)
        DO UPDATE SET
            product_name = EXCLUDED.product_name,
            product_type = EXCLUDED.product_type,
            measure_unit = EXCLUDED.measure_unit,
            turnover = EXCLUDED.turnover,
            updated_at = now();
    """

    values = [
        (
            r["department"],
            r["oper_day"],
            r["product_num"],
            r["product_name"],
            r["product_type"],
            r["measure_unit"],
            r["transaction_type"],
            r["turnover"],
        )
        for r in rows
    ]

    with conn.cursor() as cur:
        execute_values(
            cur,
            sql,
            values,
            template="(%s,%s,%s,%s,%s,%s,%s,%s,now())",
            page_size=500,
        )
    conn.commit()
    return len(rows)

def print_db_sample(conn, date_from: dt.date, date_to: dt.date):
    print("🗄️ Первые 10 строк из БД за период:")
    q = """
        SELECT
            department, oper_day, product_num, transaction_type,
            product_name, product_type, measure_unit, turnover
        FROM stock_tx_iiko
        WHERE oper_day >= %s AND oper_day < %s
        ORDER BY oper_day, department, product_num, transaction_type
        LIMIT 10;
    """
    with conn.cursor() as cur:
        cur.execute(q, (date_from, date_to))
        rows = cur.fetchall()

    for i, r in enumerate(rows, start=1):
        print(f"{i:02d}. {r}")

def main():
    date_from, date_to = get_period()
    print(f"🚀 ETL STOCK TX: {date_from} – {date_to}")
    print(f"🌐 IIKO_BASE_URL: {IIKO_BASE_URL}")

    token = get_token()
    try:
        raw_rows = fetch_stock_tx(token, date_from, date_to)

        # агрегируем, потому что document в таблице нет
        rows = aggregate_without_document(raw_rows)
        print(f"🧮 После агрегации (без document): {len(rows)} строк")

        conn = get_pg_connection()
        try:
            n = upsert_stock_tx(conn, rows)
            print(f"✅ В stock_tx_iiko upsert'нуто строк: {n}")
            print_db_sample(conn, date_from, date_to)
        finally:
            conn.close()
            print("🔌 Соединение с Postgres закрыто")
    finally:
        logout(token)

if __name__ == "__main__":
    main()
