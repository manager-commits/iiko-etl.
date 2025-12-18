import os
import datetime as dt
import requests
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()

# ========== iiko ==========
RAW_IIKO_BASE_URL = os.getenv("IIKO_BASE_URL", "")
IIKO_LOGIN = os.getenv("IIKO_LOGIN")
IIKO_PASSWORD = os.getenv("IIKO_PASSWORD")

DEPARTMENTS = ["Авиагородок", "Домодедово"]

# Если фильтр по артикулу нужен — оставь; если нет — сделай [] или закомментируй блок фильтра ниже
PRODUCT_NUM_FILTER = ["00001"]


def normalize_base_url(url: str) -> str:
    """
    Приводим базовый URL к виду без хвоста /resto и без слэша в конце.
    Для OLAP/auth/logout используем /api/*
    """
    url = (url or "").strip().rstrip("/")
    if url.endswith("/resto"):
        url = url[:-5]
    return url.rstrip("/")


IIKO_BASE_URL = normalize_base_url(RAW_IIKO_BASE_URL)


# ========== Postgres (Neon) ==========
def get_pg_connection():
    return psycopg2.connect(
        host=os.getenv("PG_HOST"),
        port=os.getenv("PG_PORT"),
        dbname=os.getenv("PG_DB"),
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD"),
        sslmode=os.getenv("PG_SSLMODE", "require"),
    )


# ========== Auth ==========
def get_token() -> str:
    if not IIKO_BASE_URL:
        raise RuntimeError("IIKO_BASE_URL is not set")
    if not IIKO_LOGIN or not IIKO_PASSWORD:
        raise RuntimeError("IIKO_LOGIN / IIKO_PASSWORD is not set")

    url = f"{IIKO_BASE_URL}/api/auth"
    resp = requests.get(url, params={"login": IIKO_LOGIN, "pass": IIKO_PASSWORD}, timeout=30)
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


# ========== Period ==========
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


# ========== DB schema helpers ==========
def get_table_columns(conn, table_name: str, schema: str = "public") -> set[str]:
    q = """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s;
    """
    with conn.cursor() as cur:
        cur.execute(q, (schema, table_name))
        return {r[0] for r in cur.fetchall()}


def pick_turnover_column(cols: set[str]) -> str:
    """
    Какое имя колонки использовать под Amount.StoreInOutTyped.
    Поддерживаем несколько вариантов, чтобы не падать из-за нейминга.
    """
    for cand in ("turnover", "store_in_out", "amount_store_in_out", "amount"):
        if cand in cols:
            return cand
    raise RuntimeError(
        "Не нашёл колонку под оборот. Ожидал одну из: turnover / store_in_out / amount_store_in_out / amount"
    )


# ========== iiko OLAP ==========
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
        "Department": {
            "filterType": "IncludeValues",
            "values": DEPARTMENTS,
        },
    }

    # Фильтр по артикулу — только если задан
    if PRODUCT_NUM_FILTER:
        filters["Product.Num"] = {
            "filterType": "IncludeValues",
            "values": PRODUCT_NUM_FILTER,
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


def aggregate_rows(rows: list[dict], with_document: bool) -> list[dict]:
    """
    Если в таблице нет document — агрегируем без него.
    """
    key_fields = ["department", "oper_day", "product_num", "product_name", "product_type", "measure_unit", "transaction_type"]
    if with_document:
        key_fields.insert(6, "document")  # перед transaction_type

    agg = {}
    for r in rows:
        key = tuple(r.get(k) for k in key_fields)
        if key not in agg:
            agg[key] = dict(r)
        else:
            agg[key]["turnover"] = float(agg[key].get("turnover") or 0) + float(r.get("turnover") or 0)

    out = list(agg.values())
    print(f"📌 После агрегации ({'с document' if with_document else 'без document'}): {len(out)} строк")
    return out


# ========== Upsert ==========
def upsert_stock_tx(conn, rows: list[dict]):
    if not rows:
        print("⚠️ Нет строк для записи в БД")
        return 0

    cols = get_table_columns(conn, "stock_tx_iiko", "public")
    has_document = "document" in cols
    turnover_col = pick_turnover_column(cols)

    # Агрегация под схему таблицы
    rows = aggregate_rows(rows, with_document=has_document)

    insert_cols = [
        "department",
        "oper_day",
        "product_num",
        "product_name",
        "product_type",
        "measure_unit",
    ]
    if has_document:
        insert_cols.append("document")
    insert_cols.append("transaction_type")
    insert_cols.append(turnover_col)
    insert_cols.append("updated_at")

    conflict_cols = ["department", "oper_day", "product_num", "transaction_type"]
    if has_document:
        conflict_cols.insert(3, "document")  # department, oper_day, product_num, document, transaction_type

    # VALUES
    values = []
    for r in rows:
        row_vals = [
            r.get("department"),
            r.get("oper_day"),
            r.get("product_num"),
            r.get("product_name"),
            r.get("product_type"),
            r.get("measure_unit"),
        ]
        if has_document:
            row_vals.append(r.get("document"))
        row_vals.append(r.get("transaction_type"))
        row_vals.append(float(r.get("turnover") or 0))
        values.append(tuple(row_vals))

    cols_sql = ",\n            ".join(insert_cols)
    conflict_sql = ", ".join(conflict_cols)

    # Обновляем справочные поля + сумму
    update_parts = [
        "product_name = EXCLUDED.product_name",
        "product_type = EXCLUDED.product_type",
        "measure_unit = EXCLUDED.measure_unit",
        f"{turnover_col} = EXCLUDED.{turnover_col}",
        "updated_at = now()",
    ]
    update_sql = ",\n            ".join(update_parts)

    sql = f"""
        INSERT INTO stock_tx_iiko (
            {cols_sql}
        )
        VALUES %s
        ON CONFLICT ({conflict_sql})
        DO UPDATE SET
            {update_sql};
    """

    # template: + now() в конце
    placeholders = ["%s"] * (len(insert_cols) - 1)  # кроме updated_at
    template = "(" + ",".join(placeholders) + ",now())"

    with conn.cursor() as cur:
        execute_values(cur, sql, values, template=template, page_size=500)

    conn.commit()
    return len(values)


def print_db_sample(conn, date_from: dt.date, date_to: dt.date):
    cols = get_table_columns(conn, "stock_tx_iiko", "public")
    has_document = "document" in cols
    turnover_col = pick_turnover_column(cols)

    print("🗄️ Первые 10 строк из БД за период:")
    select_cols = ["department", "oper_day", "product_num"]
    if has_document:
        select_cols.append("document")
    select_cols += ["transaction_type", turnover_col]

    q = f"""
        SELECT {", ".join(select_cols)}
        FROM stock_tx_iiko
        WHERE oper_day >= %s AND oper_day < %s
        ORDER BY oper_day, department, product_num
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
