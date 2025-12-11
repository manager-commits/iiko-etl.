import os
from datetime import datetime

import psycopg2
from dotenv import load_dotenv

import gspread
from oauth2client.service_account import ServiceAccountCredentials

# Загружаем переменные окружения при локальном запуске
load_dotenv()

# ---- Настройки ----

GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID")  # spreadsheetId из URL
SHEET_NAME = "ФОТ"                              # имя листа

def get_pg_connection():
    return psycopg2.connect(
        host=os.getenv("PG_HOST"),
        port=os.getenv("PG_PORT"),
        dbname=os.getenv("PG_DB"),
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD"),
        sslmode=os.getenv("PG_SSLMODE", "require"),
    )

def get_sheet():
    scope = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(
        "google_service_key.json", scope
    )
    client = gspread.authorize(creds)
    return client.open_by_key(GOOGLE_SHEET_ID).worksheet(SHEET_NAME)

def parse_date(value: str):
    """Парсим дату формата 01.11.2025 -> date."""
    try:
        return datetime.strptime(value.strip(), "%d.%m.%Y").date()
    except Exception:
        return None

def parse_num(value: str):
    """Парсим русские числа с пробелами и запятой."""
    if value is None:
        return 0
    value = str(value).strip()
    if not value:
        return 0
    value = value.replace(" ", "").replace("\u00a0", "").replace(",", ".")
    try:
        return float(value)
    except Exception:
        return 0

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
        # Ожидаем минимум 8 колонок
        if len(row) < 8:
            continue

        oper_day = parse_date(row[0])
        department = row[5].strip() if len(row) > 5 else ""

        if not oper_day or not department:
            # без даты или точки смысла нет
            continue

        item = {
            "oper_day": oper_day,
            "fot_povar":      parse_num(row[1]),
            "fot_kur":        parse_num(row[2]),
            "fot_ofis":       parse_num(row[3]),
            "fot_uborsh":     parse_num(row[4]),
            "department":     department,
            "reklama_budget": parse_num(row[6]),
            "fot_reklamy":    parse_num(row[7]),
        }
        result.append(item)

    print(f"✅ Разобрано строк: {len(result)}")
    return result

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
            fot_povar       = EXCLUDED.fot_povar,
            fot_kur         = EXCLUDED.fot_kur,
            fot_ofис        = EXCLUDED.fot_ofис,
            fot_uborsh      = EXCLUDED.fot_uborsh,
            reklama_budget  = EXCLUDED.reklama_budget,
            fot_reklamy     = EXCLUDED.fot_reklamy,
            updated_at      = now();
    """

    # опечатка в названии столбца (fot_ofис) можно поправить при необходимости,
    # но будем считать, что в БД fot_ofис написано как fot_ofis

    # заменим в запросе на корректное имя
    query = query.replace("fot_ofис", "fot_ofis")

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
