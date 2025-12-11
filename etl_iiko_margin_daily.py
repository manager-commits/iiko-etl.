import os
import requests
import psycopg2
from datetime import datetime, timedelta
import json

# === CONFIG ===
IIKO_BASE_URL = os.getenv("IIKO_BASE_URL")
IIKO_LOGIN = os.getenv("IIKO_LOGIN")
IIKO_PASSWORD = os.getenv("IIKO_PASSWORD")

PG_HOST = os.getenv("PG_HOST")
PG_PORT = os.getenv("PG_PORT")
PG_DB = os.getenv("PG_DB")
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")


def get_iiko_token():
    url = f"{IIKO_BASE_URL}/api/0/auth/access_token"
    response = requests.post(url, json={"userLogin": IIKO_LOGIN, "userPassword": IIKO_PASSWORD})
    response.raise_for_status()
    return response.json()


def fetch_margin(token, date_from, date_to, courier_only=False):
    """Загрузка маржи, courier_only=True → включаем фильтр Delivery.ServiceType = COURIER"""
    url = f"{IIKO_BASE_URL}/api/0/reports/olap"

    filters = {
        "Department": ["Авиагородок", "Домодедово"],
        "Storned": ["FALSE"],
        "DeletedWithWriteoff": ["NOT_DELETED"],
        "OrderDeleted": ["NOT_DELETED"]
    }

    if courier_only:
        filters["Delivery.ServiceType"] = ["COURIER"]

    payload = {
        "report": {
            "groupByRowFields": [
                {"field": "Department"},
                {"field": "OpenDate.Typed"}
            ],
            "aggregateFields": [
                {"field": "DishSumInt"},
                {"field": "DiscountSum"},
                {"field": "ProductCostBase.ProductCost"}
            ]
        },
        "filter": {
            "dateFrom": date_from,
            "dateTo": date_to,
            "includeLow": True,
            "includeHigh": False,
            "filters": filters
        }
    }

    response = requests.post(
        url,
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        data=json.dumps(payload)
    )
    response.raise_for_status()
    return response.json().get("data", [])


def save_to_db(rows_total, rows_courier):
    conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        database=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD
    )
    cur = conn.cursor()

    # Приводим courier-данные в словарь {(department, day): values}
    courier_map = {}
    for r in rows_courier:
        key = (r["Department"], r["OpenDate.Typed"])
        courier_map[key] = r

    for r in rows_total:
        department = r["Department"]
        oper_day = r["OpenDate.Typed"]

        revenue = r.get("DishSumInt", 0)
        discount = r.get("DiscountSum", 0)
        product_cost = r.get("ProductCostBase.ProductCost", 0)

        # Ищем курьерскую запись
        courier = courier_map.get((department, oper_day), {})

        revenue_c = courier.get("DishSumInt", 0)
        discount_c = courier.get("DiscountSum", 0)
        product_cost_c = courier.get("ProductCostBase.ProductCost", 0)

        sql = """
        INSERT INTO margin_iiko (
            department, oper_day,
            revenue, discount, product_cost,
            revenue_courier, discount_courier, product_cost_courier,
            updated_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
        ON CONFLICT (department, oper_day)
        DO UPDATE SET
            revenue = EXCLUDED.revenue,
            discount = EXCLUDED.discount,
            product_cost = EXCLUDED.product_cost,
            revenue_courier = EXCLUDED.revenue_courier,
            discount_courier = EXCLUDED.discount_courier,
            product_cost_courier = EXCLUDED.product_cost_courier,
            updated_at = NOW()
        ;
        """

        cur.execute(sql, (
            department, oper_day,
            revenue, discount, product_cost,
            revenue_c, discount_c, product_cost_c
        ))

    conn.commit()
    cur.close()
    conn.close()


def main():
    today = datetime.now()
    date_from = (today - timedelta(days=1)).strftime("%Y-%m-%d")
    date_to = today.strftime("%Y-%m-%d")

    print(f"📌 ETL MARGIN DAILY: {date_from} → {date_to}")

    token = get_iiko_token()
    access_token = token["token"]

    print("→ Загружаем общую маржу...")
    rows_total = fetch_margin(access_token, date_from, date_to, courier_only=False)
    print(f"✓ Общих строк: {len(rows_total)}")

    print("→ Загружаем маржу по доставке курьером...")
    rows_courier = fetch_margin(access_token, date_from, date_to, courier_only=True)
    print(f"✓ Курьерских строк: {len(rows_courier)}")

    print("→ Сохраняем в базу...")
    save_to_db(rows_total, rows_courier)

    print("✓ Готово!")


if __name__ == "__main__":
    main()
