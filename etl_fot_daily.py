import os
import json
from datetime import datetime

import psycopg2
from dotenv import load_dotenv

import gspread
from google.oauth2.service_account import Credentials

# Локальный запуск: подтянуть .env (в Actions это не мешает)
load_dotenv()

SHEET_NAME = "ФОТ"


# ---------- Postgres ----------
def get_pg_connection():
    return psycopg2.connect(
        host=os.getenv("PG_HOST"),
        port=os.getenv("PG_PORT"),
        dbname=os.getenv("PG_DB"),
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD"),
        sslmode=os.getenv("PG_SSLMODE", "require"),
    )


# ---------- Google Sheets (Вариант 1: creds из ENV GOOGLE_CREDENTIALS) ----------
def get_sheet_id() -> str:
    sheet_id = os.getenv("GOOGLE_SHEET_ID")
    if not sheet_id:
        raise RuntimeError("GOOGLE_SHEET_ID is not set (нужно добавить secret и пробросить в workflow env).")
    return sheet_id.strip()


def get_gspread_client():
    raw = os.getenv("GOOGLE_CREDENTIALS")
    if not raw:
        raise RuntimeError("GOOGLE_CREDENTIALS is not set (в secret должен быть JSON сервис-аккаунта).")

    try:
        info = json.loads(raw)
    except json.JSONDecodeError as e:
        raise RuntimeError(f"GOOGLE_CREDENTIALS is not valid JSON: {e}")

    scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(creds)


def get_sheet():
    client = get_gspread_client()
    sheet_id = get_sheet_id()
    return client.open_by_key(sheet_id).worksheet(SHEET_NAME)


# ---------- Парсинг ----------
def parse_date(value: str):
    if not value:
        return None
    value = str(value).strip()
    for fmt in ("%d.%m.%Y", "%d.%m.%y"):
        try:
            return datetime.strptime(value, fmt).date()
        except ValueError:
            pass
    return None


def parse_num(value):
    if value is None:
        return 0.0
    value = str(value).strip()
    if not value:
        return 0.0
    value = value.replace(" ", "").replace("\u00a0", "").replace(",", ".")
    try:
        return float(value)
    except Exception:
        return 0.0


# ---------- Extract ----------
def load_fot_data():
    print("📄 Читаем Google Sheet 'ФОТ'...")

    sheet = get_sheet()
    rows = sheet.get_all_values()

    if not rows or len(rows) < 2:
        print("⚠ В таблице нет данных")
        return []

    header = rows[0]
    data_rows = rows[1:]

    print(f"🔍 Столбцы: {header}")
    print(f"🔍 Строк данных (без заголовка): {len(data_rows)}")

    result = []

    for row in data_rows:
        # ожидаем минимум 8 колонок:
        # 0 Учетный день
        # 1 ФОТ Повара
        # 2 ФОТ Курьеры
        # 3 ФОТ Офики
        # 4 ФОТ Уборщицы
        # 5 Торговое предприятие
        # 6 Рекламный бюджет
        # 7 ФОТ Рекламы
        if len(row) < 8:
            continue

        oper_day = parse_date(row[0])
        department = row[5].strip() if row[5] else ""

        if not oper_day or not department:
            continue

        result.append(
            {
                "department": department,
                "oper_day": oper_day,
                "fot_povar": parse_num(row[1]),
                "fot_kur": parse_num(row[2]),
                "fot_ofis": parse_num(row[3]),
                "fot_uborsh": parse_num(row[4]),
                "reklama_budget": parse_num(row[6]),
                "fot_reklamy": parse_num(row[7]),
            }
        )

    print(f"✅ Разобрано строк: {len(result)}")
    return result


# ---------- Load ----------
def save_to_db(rows):
    if not rows:
        print("⚠ Нет данных для записи")
        return

    conn = get_pg_connection()
    cur = conn.cursor()

    query = """
        INSERT INTO fot_daily (
            department,
            oper_day,
            fot_povar,
            fot_kur,
            fot_ofis,
            fot_uborsh,
            reklama_budget,
            fot_reklamy,
            updated_at
        )
        VALUES (
            %(department)s,
            %(oper_day)s,
            %(fot_povar)s,
            %(fot_kur)s,
            %(fot_ofis)s,
            %(fot_uborsh)s,
            %(reklama_budget)s,
            %(fot_reklamy)s,
            now()
        )
        ON CONFLICT (department, oper_day)
        DO UPDATE SET
            fot_povar      = EXCLUDED.fot_povar,
            fot_kur        = EXCLUDED.fot_kur,
            fot_ofis       = EXCLUDED.fot_ofis,
            fot_uborsh     = EXCLUDED.fot_uborsh,
            reklama_budget = EXCLUDED.reklama_budget,
            fot_reklamy    = EXCLUDED.fot_reklamy,
            updated_at     = now();
    """

    for r in rows:
        cur.execute(query, r)

    conn.commit()
    cur.close()
    conn.close()

    print(f"💾 В fot_daily записано строк: {len(rows)}")


def main():
    rows = load_fot_data()
    save_to_db(rows)


if __name__ == "__main__":
    main()
