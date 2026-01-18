import os
import datetime as dt
import time
import requests
import psycopg2
from dotenv import load_dotenv

load_dotenv()

IIKO_BASE_URL = os.getenv("IIKO_BASE_URL", "").rstrip("/")
IIKO_LOGIN = os.getenv("IIKO_LOGIN")
IIKO_PASSWORD = os.getenv("IIKO_PASSWORD")

# Таймауты HTTP: (connect, read)
HTTP_CONNECT_TIMEOUT = int(os.getenv("HTTP_CONNECT_TIMEOUT", "20"))
HTTP_READ_TIMEOUT = int(os.getenv("HTTP_READ_TIMEOUT", "300"))

# Ретраи HTTP
HTTP_RETRIES = int(os.getenv("HTTP_RETRIES", "3"))
HTTP_RETRY_SLEEP_SEC = int(os.getenv("HTTP_RETRY_SLEEP_SEC", "5"))

# Размер чанка в днях (по умолчанию 7 = неделя)
CHUNK_DAYS = int(os.getenv("CHUNK_DAYS", "7"))


def get_pg_connection():
    return psycopg2.connect(
        host=os.getenv("PG_CRM_HOST"),
        port=os.getenv("PG_CRM_PORT"),
        dbname=os.getenv("PG_CRM_DB"),
        user=os.getenv("PG_CRM_USER"),
        password=os.getenv("PG_CRM_PASSWORD"),
        sslmode=os.getenv("PG_CRM_SSLMODE", "require"),
    )


def get_token() -> str:
    url = f"{IIKO_BASE_URL}/api/auth"
    params = {"login": IIKO_LOGIN, "pass": IIKO_PASSWORD}
    resp = requests.get(url, params=params, timeout=(HTTP_CONNECT_TIMEOUT, HTTP_READ_TIMEOUT))
    resp.raise_for_status()
    token = resp.text.strip()
    print(f"🔑 Token: {token[:6]}...")
    return token


def logout(token: str):
    url = f"{IIKO_BASE_URL}/api/logout"
    params = {"key": token}
    try:
        requests.post(url, params=params, timeout=(HTTP_CONNECT_TIMEOUT, 30))
    except Exception as e:
        print("⚠️ Logout error:", e)


def get_period():
    date_from_str = os.getenv("DATE_FROM")
    date_to_str = os.getenv("DATE_TO")

    if date_from_str and date_to_str:
        date_from = dt.date.fromisoformat(date_from_str)
        date_to = dt.date.fromisoformat(date_to_str)
        print(f"📅 Period from ENV: {date_from} -> {date_to}")
        return date_from, date_to

    # default: yesterday
    today = dt.date.today()
    d = today - dt.timedelta(days=1)
    print(f"📅 Default period: {d}")
    return d, d


def week_chunks(date_from: dt.date, date_to: dt.date, chunk_days: int = 7):
    """
    Режем на интервалы по N дней (по умолчанию 7).
    date_to включительно.
    Пример: 01-07, 08-14, ...
    """
    if chunk_days <= 0:
        chunk_days = 7

    chunks = []
    cur = date_from
    while cur <= date_to:
        end = min(cur + dt.timedelta(days=chunk_days - 1), date_to)
        chunks.append((cur, end))
        cur = end + dt.timedelta(days=1)
    return chunks


def build_olap_body(date_from: dt.date, date_to: dt.date):
    return {
        "reportType": "SALES",
        "buildSummary": False,
        "groupByRowFields": [
            "Delivery.CookingFinishTime",
            "OpenTime",
            "Delivery.PrintTime",
            "Delivery.SendTime",
            "Delivery.ActualTime",
            "Delivery.CloseTime",
            "Delivery.ExpectedTime",
            "OpenDate.Typed",
            "Delivery.SourceKey",
            "Delivery.DeliveryComment",
            "Department",
            "Delivery.Region",
            "Delivery.Number",
            "Delivery.CustomerName",
            "Delivery.Phone",
            "Delivery.Address",
            "Delivery.Courier",
        ],
        "aggregateFields": [],
        "filters": {
            "OpenDate.Typed": {
                "filterType": "DateRange",
                "periodType": "CUSTOM",
                "from": date_from.strftime("%Y-%m-%d"),
                "to": date_to.strftime("%Y-%m-%d"),
                "includeLow": True,
                "includeHigh": True,
            },
            "Storned": {"filterType": "IncludeValues", "values": ["FALSE"]},
            "DeletedWithWriteoff": {"filterType": "IncludeValues", "values": ["NOT_DELETED"]},
            "Department": {"filterType": "IncludeValues", "values": ["Авиагородок", "Домодедово"]},
            "OrderDeleted": {"filterType": "IncludeValues", "values": ["NOT_DELETED"]},
            "Delivery.CookingFinishTime": {"filterType": "ExcludeValues", "values": [None]},
            "Delivery.Courier": {"filterType": "ExcludeValues", "values": [None, "Самовывоз"]},
        },
    }


def fetch_t1_light_with_token_refresh(token_ref: dict, date_from: dt.date, date_to: dt.date) -> dict:
    """
    token_ref = {"token": "..."} — чтобы можно было обновить токен внутри функции.
    При 401 перевыпускаем токен и повторяем запрос.
    """
    url = f"{IIKO_BASE_URL}/api/v2/reports/olap"

    body = build_olap_body(date_from, date_to)

    last_err = None
    for attempt in range(1, HTTP_RETRIES + 1):
        token = token_ref["token"]
        params = {"key": token}
        try:
            print(f"📦 iiko OLAP request: {date_from} -> {date_to} (attempt {attempt}/{HTTP_RETRIES})")
            resp = requests.post(
                url,
                params=params,
                json=body,
                timeout=(HTTP_CONNECT_TIMEOUT, HTTP_READ_TIMEOUT),
            )

            print("HTTP:", resp.status_code)

            if resp.status_code == 401:
                print("🔁 401 Unauthorized — token expired/invalid. Refresh token and retry chunk...")
                try:
                    logout(token)
                except Exception:
                    pass
                token_ref["token"] = get_token()
                # повторяем этот же attempt (без sleep)
                continue

            if resp.status_code >= 400:
                print("iiko response (first 1000 chars):")
                print(resp.text[:1000])

            resp.raise_for_status()
            return resp.json()

        except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectionError) as e:
            last_err = e
            print(f"⏳ Network/timeout error: {e}")
            if attempt < HTTP_RETRIES:
                print(f"🔁 Sleep {HTTP_RETRY_SLEEP_SEC}s then retry...")
                time.sleep(HTTP_RETRY_SLEEP_SEC)
            else:
                raise
        except Exception as e:
            raise

    raise last_err


def upsert_t1_light(data: dict):
    rows = data.get("data", [])
    print(f"📊 Rows received: {len(rows)}")
    if not rows:
        print("⚠️ No data to write.")
        return

    conn = get_pg_connection()
    cur = conn.cursor()

    query = """
    INSERT INTO crm.iiko_t1_light (
        delivery_cooking_finish_time,
        open_time,
        delivery_print_time,
        delivery_send_time,
        delivery_actual_time,
        delivery_close_time,
        delivery_expected_time,
        open_date,
        delivery_source_key,
        delivery_comment,
        department,
        delivery_region,
        delivery_number,
        delivery_customer_name,
        delivery_phone,
        delivery_address,
        delivery_courier,
        updated_at
    )
    VALUES (
        %(Delivery.CookingFinishTime)s,
        %(OpenTime)s,
        %(Delivery.PrintTime)s,
        %(Delivery.SendTime)s,
        %(Delivery.ActualTime)s,
        %(Delivery.CloseTime)s,
        %(Delivery.ExpectedTime)s,
        %(OpenDate.Typed)s,
        %(Delivery.SourceKey)s,
        %(Delivery.DeliveryComment)s,
        %(Department)s,
        %(Delivery.Region)s,
        %(Delivery.Number)s,
        %(Delivery.CustomerName)s,
        %(Delivery.Phone)s,
        %(Delivery.Address)s,
        %(Delivery.Courier)s,
        now()
    )
    ON CONFLICT (department, delivery_cooking_finish_time, delivery_number)
    DO UPDATE SET
        open_time = EXCLUDED.open_time,
        delivery_print_time = EXCLUDED.delivery_print_time,
        delivery_send_time = EXCLUDED.delivery_send_time,
        delivery_actual_time = EXCLUDED.delivery_actual_time,
        delivery_close_time = EXCLUDED.delivery_close_time,
        delivery_expected_time = EXCLUDED.delivery_expected_time,
        open_date = EXCLUDED.open_date,
        delivery_source_key = EXCLUDED.delivery_source_key,
        delivery_comment = EXCLUDED.delivery_comment,
        delivery_region = EXCLUDED.delivery_region,
        delivery_customer_name = EXCLUDED.delivery_customer_name,
        delivery_phone = EXCLUDED.delivery_phone,
        delivery_address = EXCLUDED.delivery_address,
        delivery_courier = EXCLUDED.delivery_courier,
        updated_at = now();
    """

    for row in rows:
        cur.execute(query, row)

    conn.commit()
    cur.close()
    conn.close()
    print("✅ Upsert done")


def main():
    date_from, date_to = get_period()
    print(f"🚀 ETL TI Light (CRM): {date_from} -> {date_to}")

    token_ref = {"token": get_token()}
    try:
        chunks = week_chunks(date_from, date_to, chunk_days=CHUNK_DAYS) if date_from != date_to else [(date_from, date_to)]
        print(f"🧩 Chunks: {len(chunks)} (chunk_days={CHUNK_DAYS})")

        for i, (d1, d2) in enumerate(chunks, 1):
            print(f"\n=== Chunk {i}/{len(chunks)}: {d1} -> {d2} ===")
            data = fetch_t1_light_with_token_refresh(token_ref, d1, d2)
            upsert_t1_light(data)

    finally:
        try:
            logout(token_ref["token"])
        except Exception:
            pass
        print("🔐 Logout done")


if __name__ == "__main__":
    main()
