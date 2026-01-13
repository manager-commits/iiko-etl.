import os
import datetime as dt
import requests
import psycopg2
from psycopg2.extras import execute_batch

# -------------------------
# iiko creds (GitHub secrets)
# -------------------------
IIKO_BASE_URL = os.getenv("IIKO_BASE_URL", "").rstrip("/")
IIKO_LOGIN = os.getenv("IIKO_LOGIN")
IIKO_PASSWORD = os.getenv("IIKO_PASSWORD")

REPORT_ID = "1df51370-2567-40c3-8cbf-b32f966125bd"
REPORT_NAME = "Отчёт для CRM"

# -------------------------
# Neon CRM Postgres creds (new project)
# -------------------------
def get_pg_crm_connection():
    return psycopg2.connect(
        host=os.getenv("PG_CRM_HOST"),
        port=os.getenv("PG_CRM_PORT", "5432"),
        dbname=os.getenv("PG_CRM_DB", "neondb"),
        user=os.getenv("PG_CRM_USER"),
        password=os.getenv("PG_CRM_PASSWORD"),
        sslmode=os.getenv("PG_CRM_SSLMODE", "require"),
    )

# -------------------------
# Auth
# -------------------------
def get_token() -> str:
    url = f"{IIKO_BASE_URL}/api/auth"
    params = {"login": IIKO_LOGIN, "pass": IIKO_PASSWORD}
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    token = resp.text.strip()
    print(f"🔑 Token: {token[:6]}...")
    return token

def logout(token: str):
    url = f"{IIKO_BASE_URL}/api/logout"
    params = {"key": token}
    try:
        requests.post(url, params=params, timeout=10)
    except Exception as e:
        print("⚠️ logout error:", e)

# -------------------------
# Period
# DATE_FROM / DATE_TO in ISO: YYYY-MM-DD
# If not set: default yesterday -> today
# -------------------------
def get_period():
    df = os.getenv("DATE_FROM")
    dt_ = os.getenv("DATE_TO")

    if df and dt_:
        date_from = dt.date.fromisoformat(df)
        date_to = dt.date.fromisoformat(dt_)
        print(f"📅 Period from ENV: {date_from} -> {date_to}")
        return date_from, date_to

    today = dt.date.today()
    date_from = today - dt.timedelta(days=1)
    date_to = today
    print(f"📅 Default period: {date_from} -> {date_to}")
    return date_from, date_to

# -------------------------
# Fetch OLAP report
# Using known-good filter OpenDate.Typed (like your working scripts)
# -------------------------
def fetch_crm_report(token: str, date_from: dt.date, date_to: dt.date) -> dict:
    url = f"{IIKO_BASE_URL}/api/v2/reports/olap"
    params = {"key": token}

    body = {
        "reportType": "SALES",
        "buildSummary": False,

        # iiko accepts id/name (reportId is NOT accepted)
        "id": REPORT_ID,
        "name": REPORT_NAME,

        "groupByRowFields": [
            "CloseTime",
            "Delivery.CloseTime",
            "Delivery.CustomerName",
            "Delivery.Phone",
            "Delivery.CustomerPhone",
            "OrderNum",
            "TableNum",
            "Department",
            "Banquet",
            "Delivery.ServiceType",
            "OrderDiscount.Type",
            "Delivery.Email",
            "PayTypes.Combo",
            "OriginName",
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
            "DeletedWithWriteoff": {
                "filterType": "IncludeValues",
                "values": ["NOT_DELETED"],
            },
            "OrderDeleted": {
                "filterType": "IncludeValues",
                "values": ["NOT_DELETED"],
            },
            "Department": {
                "filterType": "IncludeValues",
                "values": ["Авиагородок", "Домодедово"],
            },
            "Storned": {
                "filterType": "IncludeValues",
                "values": ["FALSE"],
            },
        },
    }

    resp = requests.post(url, params=params, json=body, timeout=180)
    print("HTTP:", resp.status_code)
    print("Response head (first 500 chars):")
    print(resp.text[:500])
    resp.raise_for_status()
    return resp.json()

# -------------------------
# Upsert into Neon CRM table
# Unique key: (close_time, department, order_num, pay_types_combo)
# -------------------------
def upsert_crm_orders(data: dict):
    rows = data.get("data", [])
    print(f"📥 Rows received: {len(rows)}")

    if not rows:
        print("⚠️ No data to write")
        return

    # Prepare data tuples in the exact column order
    payload = []
    for r in rows:
        payload.append((
            r.get("Banquet"),
            r.get("CloseTime"),
            r.get("Delivery.CloseTime"),
            r.get("Delivery.CustomerName"),
            r.get("Delivery.Phone"),
            r.get("Delivery.CustomerPhone"),
            r.get("Delivery.Email"),
            r.get("Delivery.ServiceType"),
            r.get("Department"),
            r.get("OrderNum"),
            r.get("TableNum"),
            r.get("OrderDiscount.Type"),
            r.get("PayTypes.Combo"),
            r.get("OriginName"),
            r.get("DishSumInt"),
            r.get("DiscountSum"),
            r.get("ProductCostBase.ProductCost"),
        ))

    sql = """
    insert into crm.iiko_sales_crm (
        banquet,
        close_time,
        delivery_close_time,
        delivery_customer_name,
        delivery_phone,
        delivery_customer_phone,
        delivery_email,
        delivery_service_type,
        department,
        order_num,
        table_num,
        order_discount_type,
        pay_types_combo,
        origin_name,
        dish_sum_int,
        discount_sum,
        product_cost,
        loaded_at
    ) values (
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now()
    )
    on conflict (close_time, department, order_num, pay_types_combo)
    do update set
        banquet                 = excluded.banquet,
        delivery_close_time     = excluded.delivery_close_time,
        delivery_customer_name  = excluded.delivery_customer_name,
        delivery_phone          = excluded.delivery_phone,
        delivery_customer_phone = excluded.delivery_customer_phone,
        delivery_email          = excluded.delivery_email,
        delivery_service_type   = excluded.delivery_service_type,
        table_num               = excluded.table_num,
        order_discount_type     = excluded.order_discount_type,
        origin_name             = excluded.origin_name,
        dish_sum_int            = excluded.dish_sum_int,
        discount_sum            = excluded.discount_sum,
        product_cost            = excluded.product_cost,
        loaded_at               = now();
    """

    conn = get_pg_crm_connection()
    try:
        with conn.cursor() as cur:
            execute_batch(cur, sql, payload, page_size=500)
        conn.commit()
        print("✅ Upsert done")
    finally:
        conn.close()

# -------------------------
# Main
# -------------------------
def main():
    date_from, date_to = get_period()
    print(f"🚀 iiko CRM ETL: {date_from} -> {date_to}")

    token = get_token()
    try:
        data = fetch_crm_report(token, date_from, date_to)
        upsert_crm_orders(data)
    finally:
        logout(token)
        print("🔐 Logout done")

if __name__ == "__main__":
    main()
