import os
import datetime as dt
import requests
from dotenv import load_dotenv

# Загружаем переменные окружения (.env) при локальном запуске
load_dotenv()

# Настройки iiko (берём из .env или секретов GitHub)
IIKO_BASE_URL = os.getenv("IIKO_BASE_URL", "").rstrip("/")
IIKO_LOGIN = os.getenv("IIKO_LOGIN")
IIKO_PASSWORD = os.getenv("IIKO_PASSWORD")

# ID отчёта "Маржа ДМД" из айко (если понадобится отдельно)
REPORT_ID = "a25f836a-e33a-4f34-85df-5bbd8c49573f"


def get_token() -> str:
    """Получаем auth-токен iiko."""
    url = f"{IIKO_BASE_URL}/api/auth"
    params = {"login": IIKO_LOGIN, "pass": IIKO_PASSWORD}

    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()

    token = resp.text.strip()
    print(f"🔑 Токен получен: {token[:6]}...")
    return token


def logout(token: str) -> None:
    """Корректный logout из iiko."""
    url = f"{IIKO_BASE_URL}/api/logout"
    params = {"key": token}
    try:
        requests.post(url, params=params, timeout=10)
    except Exception as e:
        print("⚠️ Ошибка при logout:", e)


def get_period() -> tuple[dt.date, dt.date]:
    """
    Берём период из переменных окружения DATE_FROM / DATE_TO (YYYY-MM-DD).
    Если не заданы — по умолчанию вчерашний день.
    """
    date_from_str = os.getenv("DATE_FROM")
    date_to_str = os.getenv("DATE_TO")

    if date_from_str and date_to_str:
        date_from = dt.date.fromisoformat(date_from_str)
        date_to = dt.date.fromisoformat(date_to_str)
        print(f"📅 Используем период из ENV: {date_from} – {date_to}")
        return date_from, date_to

    today = dt.date.today()
    date_from = today - dt.timedelta(days=1)
    date_to = today - dt.timedelta(days=1)
    print(f"📅 Используем период по умолчанию (вчера): {date_from}")
    return date_from, date_to


def fetch_margin_dmd(token: str, date_from: dt.date, date_to: dt.date) -> dict:
    """
    Делаем запрос в iiko OLAP по отчёту "Маржа ДМД"
    и возвращаем сырой JSON-ответ.
    """
    print("📦 Загружаем данные 'Маржа ДМД' из iiko...")

    url = f"{IIKO_BASE_URL}/api/v2/reports/olap"
    params = {"key": token}

    body = {
        "reportType": "SALES",
        "buildSummary": False,
        "groupByRowFields": [
            "CloseTime",
            "OpenTime",
            "Department",
            "Delivery.SourceKey",
            "OrderType",
            "Delivery.Region",
        ],
        "aggregateFields": [
            "DishSumInt",
            "DiscountSum",
            "ProductCostBase.ProductCost",
        ],
        "filters": {
            # В АПИ вместо SessionID.OperDay используем OpenDate.Typed,
            # как в рабочем скрипте etl_iiko_t1_light.py
            "OpenDate.Typed": {
                "filterType": "DateRange",
                "periodType": "CUSTOM",
                "from": date_from.strftime("%Y-%m-%d"),
                "to": date_to.strftime("%Y-%m-%d"),
                "includeLow": True,
                # По ТЗ includeHigh = False → [from, to)
                "includeHigh": False,
            },
            "Storned": {
                "filterType": "IncludeValues",
                "values": ["FALSE"],  # Возврат чека = Нет
            },
            "DeletedWithWriteoff": {
                "filterType": "IncludeValues",
                "values": ["NOT_DELETED"],  # Блюдо не удалено
            },
            "Department": {
                "filterType": "IncludeValues",
                "values": ["Авиагородок", "Домодедово"],
            },
            "OrderDeleted": {
                "filterType": "IncludeValues",
                "values": ["NOT_DELETED"],  # Заказ не удален
            },
        },
        # При необходимости можно явно указывать reportId,
        # если конфиг отчёта хранится в айко:
        # "reportId": REPORT_ID,
    }

    resp = requests.post(url, params=params, json=body, timeout=90)

    print("HTTP статус iiko:", resp.status_code)
    print("Тело ответа iiko (первые 1000 символов):")
    print(resp.text[:1000])
    print("-" * 80)

    resp.raise_for_status()

    print("✅ Данные успешно получены")
    return resp.json()


def preview_rows(data: dict, limit: int = 5) -> None:
    """
    Красиво печатаем первые несколько строк и список ключей.
    """
    rows = data.get("data", [])
    print(f"📊 Количество строк в ответе: {len(rows)}")

    if not rows:
        print("⚠️ В ответе нет данных (data пустая)")
        return

    print(f"\n🔍 Первые {min(limit, len(rows))} строк:")
    for idx, row in enumerate(rows[:limit], start=1):
        print(f"\n── Строка {idx} ─────────────────────────────")
        for k, v in row.items():
            print(f"{k}: {v}")

    first_keys = sorted(rows[0].keys())
    print("\n🧩 Ключи первой строки (названия полей):")
    for k in first_keys:
        print(f"- {k}")


def main():
    date_from, date_to = get_period()
    print(f"🚀 Превью отчёта 'Маржа ДМД': {date_from} – {date_to}")

    token = get_token()
    try:
        data = fetch_margin_dmd(token, date_from, date_to)
        preview_rows(data)
    finally:
        logout(token)
        print("🔐 Logout выполнен")


if __name__ == "__main__":
    main()
