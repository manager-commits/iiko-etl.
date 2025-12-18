import os
import datetime as dt
import requests
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()

# --- Настройки iiko ---
RAW_IIKO_BASE_URL = (os.getenv("IIKO_BASE_URL") or "").strip()
IIKO_LOGIN = os.getenv("IIKO_LOGIN")
IIKO_PASSWORD = os.getenv("IIKO_PASSWORD")

DEPARTMENTS = ["Авиагородок", "Домодедово"]
PRODUCT_NUM_FILTER = ["00001"]  # как в отчёте


def normalize_base_url(url: str) -> str:
    """
    Вариант А:
    Приводим базовый URL к виду https://xxx.iiko.it/resto
    чтобы работали:
      /resto/api/auth
      /resto/api/logout
      /resto/api/v2/reports/olap
    """
    url = (url or "").strip().rstrip("/")
    if not url:
        return url
    if not url.endswith("/resto"):
        url = url + "/resto"
    return url


IIKO_BASE_URL = normalize_base_url(RAW_IIKO_BASE_URL)


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
def get_token() -> str:
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
    if not token:
        return
    try:
        url = f"{IIKO_BASE_URL}/api/logout"
        requests.post(url, params={"key": token}, timeout=10)
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
    date_to = today
    print(f"📅 Период по умолчанию (вчера): {date_from} – {date_to}")
    return date_from, date_to


# --- Забираем OLAP "Отчет по проводкам" ---
def fetch_stock_tx(token: str, date_from: dt.date, date_to: dt.date):
    print("📦 Загружаем OLAP 'Проводки по заготовкам' из iiko...")

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
        oper_raw = r.get("DateTime.DateTyped")
        oper_day = oper_raw[:10] if isinstance(oper_raw, str) else oper_raw

        rows.append(
            {
                "department": r.get("Department"),
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


def upsert_stock_tx(conn, rows):
    if not rows:
        print("⚠️ Нет строк для записи в БД")
        return 0

    sql = """
        INSERT INTO stock_tx_iiko (
            department,
            oper_day,
            product_num,
            product_name,
            product_type,
            measure_unit,
            document,
            transaction_type,
            turnover,
            updated_at
        )
        VALUES %s
        ON CONFLICT (department, oper_day, product_num, document, transaction_type)
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
            r["document"],
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
            template="(%s,%s,%s,%s,%s,%s,%s,%s,%s,now())",
            page_size=500,
        )

    conn.commit()
    return len(rows)


def print_db_sample(conn, date_from: dt.date, date_to: dt.date):
    print("🗄️ Первые 10 строк из БД за период:")
    q = """
        SELECT department, oper_day, product_num, document, transaction_type, turnover
        FROM stock_tx_iiko
        WHERE oper_day >= %s AND oper_day < %s
        ORDER BY oper_day, department, product_num, document, transaction_type
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
        rows = fetch_stock_tx(token, date_from, date_to)

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
