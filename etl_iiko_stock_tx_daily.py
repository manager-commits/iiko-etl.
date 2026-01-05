import os
import datetime as dt
import requests
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()

# ========== iiko ==========
IIKO_BASE_URL = (os.getenv("IIKO_BASE_URL", "") or "").strip().rstrip("/")
IIKO_LOGIN = os.getenv("IIKO_LOGIN")
IIKO_PASSWORD = os.getenv("IIKO_PASSWORD")

DEPARTMENTS = ["Авиагородок", "Домодедово"]
PRODUCT_NUM_FILTER = [
    "0722",
    "45700042712",
    "45700042362",
    "45700041658",
    "45700042237",
    "45700042841",
    "45700042013",
    "25551",
    "45700042089",
    "0603",
    "45700042183",
    "0607",
    "45700041955",
    "06163",
    "4570004177",
    "45700041757",
    "0617",
    "45700041956",
    "45700042665",
    "45700041625",
    "45700041762",
    "2313231233122312335",
    "00001",
    "00002",
    "00003",
]  # если не нужен — сделай []


# ---------- helpers for iiko urls ----------
def _join(base: str, path: str) -> str:
    return base.rstrip("/") + "/" + path.lstrip("/")


def iiko_api_url(path: str, use_resto: bool) -> str:
    """
    use_resto=True  -> BASE/resto/<path>
    use_resto=False -> BASE/<path>
    """
    base = IIKO_BASE_URL
    if use_resto:
        if base.endswith("/resto"):
            return _join(base, path)
        return _join(base, "/resto/" + path.lstrip("/"))
    return _join(base, path)


def request_with_resto_fallback(method: str, path: str, **kwargs):
    """
    Сначала пробуем BASE/<path>, если 404 — пробуем BASE/resto/<path>.
    """
    if not IIKO_BASE_URL:
        raise RuntimeError("IIKO_BASE_URL is not set")

    url1 = iiko_api_url(path, use_resto=False)
    resp = requests.request(method, url1, **kwargs)

    if resp.status_code == 404:
        url2 = iiko_api_url(path, use_resto=True)
        resp2 = requests.request(method, url2, **kwargs)
        return resp2, url2

    return resp, url1


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


# ========== Period ==========
def get_period():
    date_from_str = os.getenv("DATE_FROM")
    date_to_str = os.getenv("DATE_TO")

    if date_from_str and date_to_str:
        date_from = dt.date.fromisoformat(date_from_str)
        date_to = dt.date.fromisoformat(date_to_str)
        print(f"📅 Период из ENV: {date_from} — {date_to}")
        return date_from, date_to

    LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "7"))

    today = dt.date.today()
    date_from = today - dt.timedelta(days=LOOKBACK_DAYS)
    date_to = today

    print(f"📅 Период по умолчанию (последние {LOOKBACK_DAYS} дней): {date_from} — {date_to}")
    return date_from, date_to


# ========== Auth ==========
def get_token() -> str:
    if not IIKO_LOGIN or not IIKO_PASSWORD:
        raise RuntimeError("IIKO_LOGIN / IIKO_PASSWORD is not set")

    resp, used_url = request_with_resto_fallback(
        "GET",
        "/api/auth",
        params={"login": IIKO_LOGIN, "pass": IIKO_PASSWORD},
        timeout=30,
    )
    print(f"🌐 AUTH URL: {used_url}")
    resp.raise_for_status()

    token = resp.text.strip()
    print(f"🔑 Токен получен: {token[:6]}...")
    return token


def logout(token: str):
    if not token:
        return
    try:
        resp, used_url = request_with_resto_fallback(
            "POST",
            "/api/logout",
            params={"key": token},
            timeout=10,
        )
        print(f"🌐 LOGOUT URL: {used_url} ({resp.status_code})")
    except Exception as e:
        print("⚠️ Ошибка при logout:", e)


# ========== iiko OLAP ==========
def fetch_stock_tx(token: str, date_from: dt.date, date_to: dt.date):
    print("📦 Загружаем OLAP 'Отчет по проводкам' из iiko...")

    filters = {
        "DateTime.OperDayFilter": {
            "filterType": "DateRange",
            "periodType": "CUSTOM",
            "from": date_from.strftime("%Y-%m-%d"),
            "to": date_to.strftime("%Y-%m-%d"),
            "includeLow": True,
            "includeHigh": False,  # ВАЖНО: oper_day < date_to
        },
        "Department": {
            "filterType": "IncludeValues",
            "values": DEPARTMENTS,
        },
    }

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
        "aggregateFields": ["Amount.StoreInOutTyped"],
        "filters": filters,
    }

    resp, used_url = request_with_resto_fallback(
        "POST",
        "/api/v2/reports/olap",
        params={"key": token},
        json=body,
        timeout=90,
    )
    print(f"🌐 OLAP URL: {used_url}")
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


def table_exists(conn, table_name: str, schema: str = "public") -> bool:
    q = """
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = %s AND table_name = %s
        );
    """
    with conn.cursor() as cur:
        cur.execute(q, (schema, table_name))
        return bool(cur.fetchone()[0])


def pick_turnover_column(cols: set[str]) -> str:
    for cand in ("turnover", "store_in_out", "amount_store_in_out", "amount"):
        if cand in cols:
            return cand
    raise RuntimeError(
        "Не нашёл колонку под оборот. Ожидал одну из: turnover / store_in_out / amount_store_in_out / amount"
    )


def aggregate_rows(rows: list[dict], with_document: bool) -> list[dict]:
    key_fields = ["department", "oper_day", "product_num", "product_name", "product_type", "measure_unit"]
    if with_document:
        key_fields.append("document")
    key_fields.append("transaction_type")

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

    if not has_document:
        raise RuntimeError("В stock_tx_iiko нет колонки document — текущая стратегия уникальности невозможна")

    rows = aggregate_rows(rows, with_document=True)

    # Разделяем потоки: с документом и без документа
    rows_with_doc = [r for r in rows if r.get("document") not in (None, "")]
    rows_no_doc = [r for r in rows if r.get("document") in (None, "")]

    insert_cols = [
        "department",
        "oper_day",
        "product_num",
        "product_name",
        "product_type",
        "measure_unit",
        "document",
        "transaction_type",
        turnover_col,
        "updated_at",
    ]
    cols_sql = ", ".join(insert_cols)

    placeholders = ["%s"] * (len(insert_cols) - 1)
    template = "(" + ",".join(placeholders) + ",now())"

    total_written = 0

    # ---------- 1) UPSERT для строк С документом ----------
    # ВАЖНО:
    # - конфликт таргет: (department, product_num, document, transaction_type) WHERE document IS NOT NULL
    # - при апдейте обновляем oper_day (документ мог "переехать" на другую дату)
    if rows_with_doc:
        values = []
        for r in rows_with_doc:
            values.append(
                (
                    r.get("department"),
                    r.get("oper_day"),
                    r.get("product_num"),
                    r.get("product_name"),
                    r.get("product_type"),
                    r.get("measure_unit"),
                    r.get("document"),
                    r.get("transaction_type"),
                    float(r.get("turnover") or 0),
                )
            )

        sql_with_doc = f"""
            INSERT INTO stock_tx_iiko ({cols_sql})
            VALUES %s
            ON CONFLICT (department, product_num, document, transaction_type)
            WHERE document IS NOT NULL
            DO UPDATE SET
                oper_day = EXCLUDED.oper_day,
                product_name = EXCLUDED.product_name,
                product_type = EXCLUDED.product_type,
                measure_unit = EXCLUDED.measure_unit,
                {turnover_col} = EXCLUDED.{turnover_col},
                updated_at = now();
        """

        with conn.cursor() as cur:
            execute_values(cur, sql_with_doc, values, template=template, page_size=500)

        conn.commit()
        total_written += len(values)
        print(f"✅ upsert (by doc key) записано: {len(values)}")

    # ---------- 2) UPSERT для строк БЕЗ документа ----------
    # Тут оставляем “старую” привязку к oper_day, потому что документ = NULL (уникализировать нечем)
    if rows_no_doc:
        values = []
        for r in rows_no_doc:
            # document кладём как None
            values.append(
                (
                    r.get("department"),
                    r.get("oper_day"),
                    r.get("product_num"),
                    r.get("product_name"),
                    r.get("product_type"),
                    r.get("measure_unit"),
                    None,
                    r.get("transaction_type"),
                    float(r.get("turnover") or 0),
                )
            )

        sql_no_doc = f"""
            INSERT INTO stock_tx_iiko ({cols_sql})
            VALUES %s
            ON CONFLICT (department, oper_day, product_num, document, transaction_type)
            DO UPDATE SET
                product_name = EXCLUDED.product_name,
                product_type = EXCLUDED.product_type,
                measure_unit = EXCLUDED.measure_unit,
                {turnover_col} = EXCLUDED.{turnover_col},
                updated_at = now();
        """

        with conn.cursor() as cur:
            execute_values(cur, sql_no_doc, values, template=template, page_size=500)

        conn.commit()
        total_written += len(values)
        print(f"✅ upsert (by day key, doc=NULL) записано: {len(values)}")

    return total_written


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


# ========== Refresh DataLens vitrine ==========
def refresh_datalens_tail(conn, date_from: dt.date, date_to: dt.date):
    """
    Пересчитываем витрину batch_daily_lifecycle сразу после загрузки stock_tx_iiko.

    В OLAP у нас oper_day в диапазоне: date_from <= oper_day < date_to
    Для snapshot_day это означает: date_from .. (date_to - 1) включительно.
    """
    snapshot_from = date_from
    snapshot_to = date_to - dt.timedelta(days=1)

    if snapshot_to < snapshot_from:
        print("⚠️ Период слишком короткий для пересчёта витрины, пропускаю")
        return

    print(f"🧮 Пересчёт витрины batch_daily_lifecycle: {snapshot_from} — {snapshot_to}")

    with conn.cursor() as cur:
        # 1) Пробуем пересчёт по диапазону, если процедура есть
        try:
            cur.execute("CALL public.refresh_batch_daily_lifecycle_range(%s, %s);", (snapshot_from, snapshot_to))
            conn.commit()
            print("✅ Витрина пересчитана через refresh_batch_daily_lifecycle_range")
            return
        except Exception as e:
            conn.rollback()
            print("⚠️ Не удалось вызвать refresh_batch_daily_lifecycle_range, пробую fallback:", str(e)[:300])

        # 2) Fallback: пересчёт “хвоста” по p_days
        p_days = (snapshot_to - snapshot_from).days + 1
        cur.execute("CALL public.refresh_batch_daily_lifecycle(%s);", (p_days,))
        conn.commit()
        print(f"✅ Витрина пересчитана через refresh_batch_daily_lifecycle(p_days => {p_days})")


# ========== Refresh anchor diffs (Plan/Fact discrepancies) ==========
def refresh_anchor_discrepancies(conn):
    """
    ВАЖНОЕ ПОВЕДЕНИЕ (как ты просишь):
    - таблицы расхождений ПОЛНОСТЬЮ пересобираются каждый запуск ETL
    - если якоря удалили/изменили -> расхождения исчезают/меняются сразу
    - если якорей нет -> таблицы расхождений становятся пустыми (а не “залипают” со старыми значениями)
    """

    # Если таблиц ещё нет — ничего не делаем (но ETL не падает).
    if not table_exists(conn, "batch_manual_anchor", "public"):
        print("ℹ️ batch_manual_anchor не существует — пропускаю пересчёт расхождений")
        return
    if not table_exists(conn, "batch_anchor_diff", "public"):
        print("ℹ️ batch_anchor_diff не существует — пропускаю пересчёт расхождений")
        return
    if not table_exists(conn, "batch_anchor_diff_total", "public"):
        print("ℹ️ batch_anchor_diff_total не существует — пропускаю пересчёт расхождений")
        return
    if not table_exists(conn, "batch_daily_lifecycle", "public"):
        print("ℹ️ batch_daily_lifecycle не существует — пропускаю пересчёт расхождений")
        return

    diff_cols = get_table_columns(conn, "batch_anchor_diff", "public")
    total_cols = get_table_columns(conn, "batch_anchor_diff_total", "public")

    # Соберём INSERT-колонки максимально “гибко” (не ломаемся, если у тебя немного другое имя поля)
    # Источник:
    #   fact:  public.batch_manual_anchor (qty_fact)
    #   plan:  public.batch_daily_lifecycle (qty_closing на anchor_day по production_day)
    # diff = fact - plan
    def pick_one(cols: set[str], variants: list[str], required: bool = False) -> str | None:
        for v in variants:
            if v in cols:
                return v
        if required:
            raise RuntimeError(f"Не нашёл обязательную колонку среди {variants}. Есть: {sorted(cols)}")
        return None

    # batch_anchor_diff columns (варианты на всякий)
    c_department = pick_one(diff_cols, ["department"], required=True)
    c_product_num = pick_one(diff_cols, ["product_num"], required=True)
    c_product_name = pick_one(diff_cols, ["product_name"])
    c_anchor_day = pick_one(diff_cols, ["anchor_day"], required=True)
    c_production_day = pick_one(diff_cols, ["production_day"], required=True)

    c_qty_fact = pick_one(diff_cols, ["qty_fact", "fact_qty"], required=True)
    c_qty_plan = pick_one(diff_cols, ["qty_plan", "plan_qty", "qty_planned"])
    c_diff_qty = pick_one(diff_cols, ["diff_qty", "qty_diff", "delta_qty"])

    c_batch_status = pick_one(diff_cols, ["batch_status", "status"])
    c_qty_closing = pick_one(diff_cols, ["qty_closing"])  # если вдруг решили хранить plan тут же
    c_qty_opening = pick_one(diff_cols, ["qty_opening"])
    c_created_at = pick_one(diff_cols, ["created_at"])
    c_updated_at = pick_one(diff_cols, ["updated_at"])

    # batch_anchor_diff_total columns
    tc_department = pick_one(total_cols, ["department"], required=True)
    tc_product_num = pick_one(total_cols, ["product_num"], required=True)
    tc_product_name = pick_one(total_cols, ["product_name"])
    tc_anchor_day = pick_one(total_cols, ["anchor_day"], required=True)

    tc_qty_fact = pick_one(total_cols, ["qty_fact_total", "qty_fact", "fact_qty_total", "fact_qty"])
    tc_qty_plan = pick_one(total_cols, ["qty_plan_total", "qty_plan", "plan_qty_total", "plan_qty"])
    tc_diff_qty = pick_one(total_cols, ["diff_qty_total", "diff_qty", "qty_diff_total", "qty_diff", "delta_qty_total", "delta_qty"])
    tc_created_at = pick_one(total_cols, ["created_at"])
    tc_updated_at = pick_one(total_cols, ["updated_at"])

    print("🧩 Пересчёт таблиц расхождений (Plan/Fact) по якорям...")

    with conn.cursor() as cur:
        # 0) Полностью очищаем таблицы расхождений — это ключ к твоему требованию “убрал якоря -> всё исчезло”
        try:
            cur.execute("TRUNCATE TABLE public.batch_anchor_diff;")
            cur.execute("TRUNCATE TABLE public.batch_anchor_diff_total;")
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise RuntimeError(f"Не смог TRUNCATE таблицы расхождений: {e}")

        # 1) Если якорей нет — оставляем таблицы пустыми и выходим
        cur.execute("SELECT COUNT(*) FROM public.batch_manual_anchor;")
        anchors_cnt = int(cur.fetchone()[0])
        if anchors_cnt == 0:
            conn.commit()
            print("✅ Якорей нет — таблицы расхождений оставлены пустыми")
            return

        # 2) Собираем detail-расхождения
        # plan берём из batch_daily_lifecycle: qty_closing на snapshot_day=anchor_day
        # Если строки в lifecycle нет — считаем план = 0 (иначе будет NULL и расчёт ломается)
        insert_cols = [c_department, c_product_num]
        select_exprs = ["a.department", "a.product_num"]

        if c_product_name:
            insert_cols.append(c_product_name)
            # берём имя из якоря, но если вдруг NULL — попробуем из prep_items_ref по canon_product_num
            select_exprs.append(
                "COALESCE(a.product_name, ref.product_name)"
            )

        insert_cols += [c_anchor_day, c_production_day, c_qty_fact]
        select_exprs += ["a.anchor_day", "a.production_day", "a.qty_fact"]

        # план / дифф (если колонки существуют)
        # qty_plan
        if c_qty_plan:
            insert_cols.append(c_qty_plan)
            select_exprs.append("COALESCE(l.qty_closing, 0)::numeric")

        # diff_qty
        if c_diff_qty:
            insert_cols.append(c_diff_qty)
            select_exprs.append("(a.qty_fact - COALESCE(l.qty_closing, 0))::numeric")

        # доп. поля (если есть)
        if c_batch_status:
            insert_cols.append(c_batch_status)
            select_exprs.append("l.batch_status")

        if c_qty_opening:
            insert_cols.append(c_qty_opening)
            select_exprs.append("COALESCE(l.qty_opening, 0)::numeric")

        if c_qty_closing:
            insert_cols.append(c_qty_closing)
            select_exprs.append("COALESCE(l.qty_closing, 0)::numeric")

        if c_created_at:
            insert_cols.append(c_created_at)
            select_exprs.append("now()")

        if c_updated_at:
            insert_cols.append(c_updated_at)
            select_exprs.append("now()")

        sql_detail = f"""
            INSERT INTO public.batch_anchor_diff ({", ".join(insert_cols)})
            SELECT
                {", ".join(select_exprs)}
            FROM public.batch_manual_anchor a
            LEFT JOIN public.batch_daily_lifecycle l
              ON l.department = a.department
             AND l.product_num = a.product_num
             AND l.snapshot_day = a.anchor_day
             AND l.production_day = a.production_day
            LEFT JOIN public.prep_items_ref ref
              ON public.canon_product_num(ref.product_num) = public.canon_product_num(a.product_num);
        """

        try:
            cur.execute(sql_detail)
            conn.commit()
            print("✅ batch_anchor_diff пересобрана")
        except Exception as e:
            conn.rollback()
            raise RuntimeError(f"Ошибка пересчёта batch_anchor_diff: {e}")

        # 3) Собираем total-расхождения (агрегат по товару на anchor_day)
        total_insert_cols = [tc_department, tc_product_num]
        total_select_exprs = ["d.department", "d.product_num"]

        if tc_product_name:
            total_insert_cols.append(tc_product_name)
            # в detail product_name может быть NULL — берём MAX/COALESCE
            total_select_exprs.append("MAX(d.product_name)")

        total_insert_cols.append(tc_anchor_day)
        total_select_exprs.append("d.anchor_day")

        # факт/план/дифф — только если колонки есть
        if tc_qty_fact:
            total_insert_cols.append(tc_qty_fact)
            # detail.qty_fact гарантированно есть (по нашему обязательному c_qty_fact)
            total_select_exprs.append("SUM(d.qty_fact)::numeric")

        if tc_qty_plan:
            total_insert_cols.append(tc_qty_plan)
            if c_qty_plan:
                total_select_exprs.append("SUM(d.qty_plan)::numeric")
            else:
                # если detail не хранит qty_plan, то пересчитаем план через join на lifecycle прямо тут
                total_select_exprs.append(
                    "SUM(COALESCE(l.qty_closing, 0))::numeric"
                )

        if tc_diff_qty:
            total_insert_cols.append(tc_diff_qty)
            if c_diff_qty:
                total_select_exprs.append("SUM(d.diff_qty)::numeric")
            else:
                # diff = fact - plan
                total_select_exprs.append(
                    "(SUM(d.qty_fact) - SUM(COALESCE(l.qty_closing, 0)))::numeric"
                )

        if tc_created_at:
            total_insert_cols.append(tc_created_at)
            total_select_exprs.append("now()")

        if tc_updated_at:
            total_insert_cols.append(tc_updated_at)
            total_select_exprs.append("now()")

        # Источник для total:
        # - если в detail есть qty_plan/diff_qty — просто агрегируем detail
        # - если нет — подтягиваем lifecycle вторым join’ом (чтобы посчитать план)
        if c_qty_plan or c_diff_qty:
            sql_total = f"""
                INSERT INTO public.batch_anchor_diff_total ({", ".join(total_insert_cols)})
                SELECT
                    {", ".join(total_select_exprs)}
                FROM public.batch_anchor_diff d
                GROUP BY d.department, d.product_num, d.anchor_day;
            """
        else:
            sql_total = f"""
                INSERT INTO public.batch_anchor_diff_total ({", ".join(total_insert_cols)})
                SELECT
                    {", ".join(total_select_exprs)}
                FROM public.batch_anchor_diff d
                LEFT JOIN public.batch_daily_lifecycle l
                  ON l.department = d.department
                 AND l.product_num = d.product_num
                 AND l.snapshot_day = d.anchor_day
                 AND l.production_day = d.production_day
                GROUP BY d.department, d.product_num, d.anchor_day;
            """

        try:
            cur.execute(sql_total)
            conn.commit()
            print("✅ batch_anchor_diff_total пересобрана")
        except Exception as e:
            conn.rollback()
            raise RuntimeError(f"Ошибка пересчёта batch_anchor_diff_total: {e}")


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

            # ✅ 1) Обновляем витрину для DataLens
            refresh_datalens_tail(conn, date_from, date_to)

            # ✅ 2) СРАЗУ ПОСЛЕ витрины пересобираем таблицы расхождений по якорям
            #    (полная пересборка каждый запуск -> удалил якоря -> расхождения исчезли)
            refresh_anchor_discrepancies(conn)

        finally:
            conn.close()
            print("🔌 Соединение с Postgres закрыто")
    finally:
        logout(token)


if __name__ == "__main__":
    main()
