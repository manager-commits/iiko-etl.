import os
import json
import csv
import datetime as dt
import requests

IIKO_BASE_URL = os.getenv("IIKO_BASE_URL", "").rstrip("/")
IIKO_LOGIN = os.getenv("IIKO_LOGIN")
IIKO_PASSWORD = os.getenv("IIKO_PASSWORD")

REPORT_ID = "1df51370-2567-40c3-8cbf-b32f966125bd"
SAMPLE_LIMIT = int(os.getenv("SAMPLE_LIMIT", "100"))

def get_token() -> str:
    url = f"{IIKO_BASE_URL}/api/auth"
    params = {"login": IIKO_LOGIN, "pass": IIKO_PASSWORD}
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    return resp.text.strip()

def logout(token: str):
    url = f"{IIKO_BASE_URL}/api/logout"
    params = {"key": token}
    try:
        requests.post(url, params=params, timeout=10)
    except Exception as e:
        print("⚠️ logout error:", e)

def get_period():
    # DATE_FROM / DATE_TO в ISO: 2026-01-11
    df = os.getenv("DATE_FROM")
    dt_ = os.getenv("DATE_TO")

    if df and dt_:
        date_from = dt.date.fromisoformat(df)
        date_to = dt.date.fromisoformat(dt_)
        return date_from, date_to

    # По умолчанию: вчера и сегодня (эксклюзивно по верхней границе)
    today = dt.date.today()
    date_from = today - dt.timedelta(days=1)
    date_to = today
    return date_from, date_to

def fetch_crm_report(token: str, date_from: dt.date, date_to: dt.date) -> dict:
    url = f"{IIKO_BASE_URL}/api/v2/reports/olap"
    params = {"key": token}

    body = {
        # тип отчёта: продажи
        "reportType": "SALES",
        "buildSummary": False,

        # группировки (строки)
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

        # агрегации
        "aggregateFields": [
            "DishSumInt",
            "DiscountSum",
            "ProductCostBase.ProductCost",
        ],

        # фильтры
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

        # иногда iiko ждёт reportId для кастомных отчётов — кладём сразу
        "id": REPORT_ID,
"name": "Отчёт для CRM",
    }

    resp = requests.post(url, params=params, json=body, timeout=120)
    print("HTTP:", resp.status_code)
    print("Response head (first 800 chars):")
    print(resp.text[:800])
    resp.raise_for_status()
    return resp.json()

def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)

def write_artifacts(data: dict):
    rows = data.get("data", [])
    total = len(rows)
    print(f"Rows received: {total}")

    sample = rows[:SAMPLE_LIMIT]
    ensure_dir("artifacts")

    # JSON
    with open("artifacts/crm_sample.json", "w", encoding="utf-8") as f:
        json.dump(sample, f, ensure_ascii=False, indent=2)

    # CSV (по ключам первой строки)
    if sample:
        fieldnames = sorted(sample[0].keys())
        with open("artifacts/crm_sample.csv", "w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames)
            w.writeheader()
            for r in sample:
                w.writerow({k: r.get(k) for k in fieldnames})

        print("Sample keys:")
        print(fieldnames)
    else:
        print("⚠️ Sample empty, nothing to write")

def main():
    date_from, date_to = get_period()
    print(f"Period: {date_from} -> {date_to} (to is exclusive, includeHigh=False)")

    token = get_token()
    try:
        data = fetch_crm_report(token, date_from, date_to)
        write_artifacts(data)
    finally:
        logout(token)

if __name__ == "__main__":
    main()
