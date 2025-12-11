import os
import psycopg2
from dotenv import load_dotenv
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime

load_dotenv()

# Настройки Google Sheets API
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID")  # spreadsheetId
SHEET_NAME = "ФОТ"

# Настройки Postgres (Neon)
def get_pg():
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
    creds = ServiceAccountCredentials.from_json_keyfile_name("google_service_key.json", scope)
    client = gspread.authorize(creds)
    return client.open_by_key(GOOGLE_SHEET_ID).worksheet(SHEET_NAME)

def parse_date(x):
    try:
        return datetime.strptime(x, "%d.%m.%Y").date()
    except:
        return None

def main():
    print("📄 Читаем Google Sheet FOT...")

    sheet = get_sheet()
    rows = sheet.get_all_values()

    header = rows[0]
    data = rows[1:]

    print(f"🔍 Найдено строк (без заголовка): {len(data)}")

    parsed_rows = []

    for r in data:
        if not r[0]:
            continue

        parsed_rows.append({
            "oper_day": parse_date(r[0]),
            "fot_povar": float(r[1].replace(" ", "").replace(",", ".")) if r[1] else 0,
            "fot_kur": float(r[2].replace(" ", "").replace(",", ".")) if r[2] else 0,
            "fot_ofiki": float(r[3].replace(" ", "").replace(",", ".")) if r[3] else 0,
            "fot_uborsh": float(r[4].replace(" ", "").replace(",", ".")) if r[4] else 0,
            "department": r[5],
            "adv_budget": float(r[6].replace(" ", "").replace(",", ".")) if r[6] else 0,
            "fot_adv": float(r[7].replace(" ", "").replace(",", ".")) if r[7] else 0,
        })

    print("📦 Загружаем данные в Neon...")

    conn = get_pg()
    cur = conn.cursor()

    query = """
        INSERT INTO fot_daily (
            oper_day,
            fot_povar,
            fot_kur,
            fot_ofiki,
            fot_uborsh,
            department,
            adv_budget,
            fot_adv,
            updated_at
        )
        VALUES (
            %(oper_day)s,
            %(fot_povar)s,
            %(fot_kur)s,
            %(fot_ofiki)s,
            %(fot_uborsh)s,
            %(department)s,
            %(adv_budget)s,
            %(fot_adv)s,
            now()
        )
        ON CONFLICT (department, oper_day)
        DO UPDATE SET
            fot_povar  = EXCLUDED.fot_povar,
            fot_kur    = EXCLUDED.fot_kur,
            fot_ofiki  = EXCLUDED.fot_ofiki,
            fot_uborsh = EXCLUDED.fot_uborsh,
            adv_budget = EXCLUDED.adv_budget,
            fot_adv    = EXCLUDED.fot_adv,
            updated_at = now();
    """

    for row in parsed_rows:
        cur.execute(query, row)

    conn.commit()
    cur.close()
    conn.close()

    print("✅ Данные успешно загружены!")

if __name__ == "__main__":
    main()
