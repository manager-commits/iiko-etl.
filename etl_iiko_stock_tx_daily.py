import os
import requests
from datetime import datetime, timedelta
from dotenv import load_dotenv

# =========================
# ENV / CONFIG
# =========================

load_dotenv()

IIKO_BASE_URL = os.getenv("IIKO_BASE_URL")
IIKO_LOGIN = os.getenv("IIKO_LOGIN")
IIKO_PASSWORD = os.getenv("IIKO_PASSWORD")

DATE_FROM = os.getenv("DATE_FROM")
DATE_TO = os.getenv("DATE_TO")

DEPARTMENTS = ["Авиагородок", "Домодедово"]
PRODUCT_NUMS = ["00001"]

# =========================
# UTILS
# =========================

def parse_date_range():
    if DATE_FROM and DATE_TO:
        date_from = datetime.strptime(DATE_FROM, "%Y-%m-%d")
        date_to = datetime.strptime(DATE_TO, "%Y-%m-%d")
    else:
        yesterday = datetime.now() - timedelta(days=1)
        date_from = yesterday.replace(hour=0, minute=0, second=0)
        date_to = date_from + timedelta(days=1)

    print(f"📅 Период: {date_from.date()} → {date_to.date()}")
    return date_from, date_to


def print_sample(rows, n=10):
    print(f"\n🧾 SAMPLE: первые {min(n, len(rows))} строк из {len(rows)}\n")
    for i, r in enumerate(rows[:n], 1):
        print(f"{i:02d}. {r}")
    print("")


# =========================
# IIKO AUTH
# =========================

def get_token():
    url = f"{IIKO_BASE_URL}/resto/api/auth"
    r = requests.get(url, params={
        "login": IIKO_LOGIN,
        "password": IIKO_PASSWORD
    })
    r.raise_for_status()
    token = r.text.strip()
    print(f"🔐 Token получен: {token[:6]}***")
    return token


# =========================
# OLAP LOAD
# =========================

def load_stock_transactions(token, date_from, date_to):
    print("📊 Загружаем OLAP 'Отчет по проводкам' из iiko...")

    url = f"{IIKO_BASE_URL}/resto/api/v2/reports/olap"

    payload = {
        "reportType": "SALES",
        "groupByRowFields": [
            "DateTime.DateTyped",
            "Product.Num",
            "Product.Name",
            "Department",
            "Product.Type",
            "Product.MeasureUnit",
            "Document",
            "TransactionType"
        ],
        "aggregateFields": [
            "Amount.StoreInOutTyped"
        ],
        "filters": [
            {
                "field": "DateTime.OperDayFilter",
                "filterType": "DateRange",
                "from": date_from.isoformat(),
                "to": date_to.isoformat(),
                "includeLow": True,
                "includeHigh": False
            },
            {
                "field": "Product.Num",
                "filterType": "IncludeValues",
                "values": PRODUCT_NUMS
            },
            {
                "field": "Department",
                "filterType": "IncludeValues",
                "values": DEPARTMENTS
            }
        ]
    }

    r = requests.post(
        url,
        headers={"Authorization": f"Bearer {token}"},
        json=payload,
        timeout=120
    )
    r.raise_for_status()

    data = r.json()
    rows = []

    for row in data.get("data", []):
        rows.append({
            "oper_day": row.get("DateTime.DateTyped"),
            "product_num": row.get("Product.Num"),
            "product_name": row.get("Product.Name"),
            "department": row.get("Department"),
            "product_type": row.get("Product.Type"),
            "measure_unit": row.get("Product.MeasureUnit"),
            "document": row.get("Document"),
            "transaction_type": row.get("TransactionType"),
            "turnover_amount": row.get("Amount.StoreInOutTyped"),
        })

    print(f"✅ Получено строк: {len(rows)}")
    return rows


# =========================
# MAIN
# =========================

def main():
    date_from, date_to = parse_date_range()
    token = get_token()
    rows = load_stock_transactions(token, date_from, date_to)

    # 🔥 ВАЖНО: печатаем первые 10 строк
    print_sample(rows, n=10)

    print("🚪 Logout выполнен")


if __name__ == "__main__":
    main()
