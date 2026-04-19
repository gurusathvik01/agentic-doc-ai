from fastapi import APIRouter, Body
from fastapi.responses import JSONResponse
import pandas as pd
import time
import json
import asyncio
import hashlib

from backend.db import query_mysql, query_mongo
from backend.pdf_handler import query_pdf

from backend.source_map import SOURCE_MAP
from backend.filter_engine import (
    build_mysql_where,
    build_mongo_filter,
    apply_csv_filter,
    apply_pdf_filter
)

from backend.data_catalog import get_data_catalog
from backend.query_planner import generate_query_from_llm

router = APIRouter()

# =========================
# CACHE
# =========================
CACHE = {}
CACHE_TTL = 60


def make_cache_key(data):
    try:
        return hashlib.md5(json.dumps(data, sort_keys=True).encode()).hexdigest()
    except Exception:
        return str(hash(str(data)))


def get_cache(key):
    if key in CACHE:
        value, ts = CACHE[key]
        if time.time() - ts < CACHE_TTL:
            return value
    return None


def set_cache(key, value):
    CACHE[key] = (value, time.time())


# =========================
# FILTER SPLIT
# =========================
def split_filters(table_selection):
    mysql_filters, mongo_filters, csv_filters, pdf_filters = {}, {}, {}, {}

    for _, cols in table_selection.items():
        if not isinstance(cols, dict):
            continue

        for col, val in cols.items():
            source = SOURCE_MAP.get(col)

            if source == "mysql":
                mysql_filters[col] = val
            elif source == "mongo":
                mongo_filters[col] = val
            elif source == "csv":
                csv_filters[col] = val
            elif source == "pdf":
                pdf_filters[col] = val

    return mysql_filters, mongo_filters, csv_filters, pdf_filters


# =========================
# SOURCE DETECTION (FIXED)
# =========================
def detect_needed_sources(columns, joins, aggregations):
    needed = set()

    for col in columns:
        if col in SOURCE_MAP:
            needed.add(SOURCE_MAP[col])

    for agg in aggregations.values():
        col = agg.get("column")
        if col in SOURCE_MAP:
            needed.add(SOURCE_MAP[col])

    for join in joins:
        for side in ["left", "right"]:
            for item in join.get(side, []):
                col = item.split(".")[1]
                if col in SOURCE_MAP:
                    needed.add(SOURCE_MAP[col])

    return needed or {"mysql"}


# =========================
# FETCHERS
# =========================
async def fetch_mysql(filters):
    try:
        query = "SELECT * FROM Employees_Master"
        where = build_mysql_where(filters)
        if where:
            query += f" WHERE {where}"
        return query_mysql(query)
    except Exception as e:
        return {"error": f"MySQL error: {str(e)}"}


async def fetch_mongo(filters):
    try:
        mongo_query = build_mongo_filter(filters) if filters else {}
        return query_mongo("users_data", mongo_query)
    except Exception as e:
        return {"error": f"Mongo error: {str(e)}"}


async def fetch_csv(filters):
    try:
        df = pd.read_csv("backend/data/sales_extra.csv")
        if filters:
            df = apply_csv_filter(df, filters)
        return df.to_dict("records")
    except Exception as e:
        return {"error": f"CSV error: {str(e)}"}


async def fetch_pdf(filters):
    try:
        data = query_pdf()
        if filters:
            data = apply_pdf_filter(data, filters)
        return data
    except Exception as e:
        return {"error": f"PDF error: {str(e)}"}


# =========================
# JOIN ENGINE (FULL FIX)
# =========================
def perform_joins(data_sources, joins):
    datasets = {k: v for k, v in data_sources.items() if isinstance(v, list)}

    if not joins:
        return sum(datasets.values(), [])

    result = None

    # pick initial dataset
    for data in datasets.values():
        if data:
            result = data
            break

    if not result:
        return []

    for join in joins:
        left_keys = [k.split(".")[1].lower() for k in join.get("left", [])]
        right_keys = [k.split(".")[1].lower() for k in join.get("right", [])]

        right_dataset = None

        # find correct dataset based on key match
        for data in datasets.values():
            if data is result or not data:
                continue

            sample = data[0]
            if all(k in sample for k in right_keys):
                right_dataset = data
                break

        if not right_dataset:
            continue

        # build hash map
        right_map = {}
        for row in right_dataset:
            key = tuple(row.get(k) for k in right_keys)
            right_map.setdefault(key, []).append(row)

        new_result = []
        for row in result:
            key = tuple(row.get(k) for k in left_keys)
            matches = right_map.get(key, [])

            for match in matches:
                merged = {**row, **match}
                new_result.append(merged)

        result = new_result

    return result


# =========================
# NORMALIZATION
# =========================
def normalize_columns(data):
    return [{k.lower(): v for k, v in row.items()} for row in data]


# =========================
# DERIVED FIELDS (NEW)
# =========================
def apply_derived_fields(data, derived_fields):
    if not derived_fields:
        return data

    for row in data:
        for new_col, expr in derived_fields.items():
            try:
                row[new_col.lower()] = eval(expr, {}, row)
            except Exception:
                row[new_col.lower()] = None

    return data


# =========================
# PROJECT
# =========================
def project_columns(data, columns):
    columns = [c.lower() for c in columns]
    return [{col: row.get(col) for col in columns} for row in data]


# =========================
# FILTERS
# =========================
def apply_advanced_filters(data, filters):
    if not filters:
        return data

    df = pd.DataFrame(data)

    try:
        for f in filters:
            col = f.get("column", "").lower()
            op = f.get("op")
            val = f.get("value")

            if col not in df.columns:
                continue

            if op == ">":
                df = df[df[col] > val]
            elif op == "<":
                df = df[df[col] < val]
            elif op == ">=":
                df = df[df[col] >= val]
            elif op == "<=":
                df = df[df[col] <= val]
            elif op == "=":
                df = df[df[col] == val]
            elif op == "!=":
                df = df[df[col] != val]
            elif op == "in":
                df = df[df[col].isin(val)]

    except Exception as e:
        print("FILTER ERROR:", e)

    return df.to_dict("records")


# =========================
# TIME FILTER
# =========================
def apply_time_filter(data, time_filter):
    if not time_filter:
        return data

    df = pd.DataFrame(data)

    try:
        col = time_filter.get("column", "").lower()
        ttype = time_filter.get("type")

        if col not in df.columns:
            return data

        df[col] = pd.to_datetime(df[col], errors="coerce")
        now = pd.Timestamp.now()

        if ttype == "last_7_days":
            df = df[df[col] >= now - pd.Timedelta(days=7)]
        elif ttype == "last_30_days":
            df = df[df[col] >= now - pd.Timedelta(days=30)]
        elif ttype == "last_month":
            df = df[df[col].dt.month == (now.month - 1)]

    except Exception as e:
        print("TIME ERROR:", e)

    return df.to_dict("records")


# =========================
# ORDER + LIMIT
# =========================
def apply_order_limit(data, order_by, limit):
    if not data:
        return data

    df = pd.DataFrame(data)

    try:
        if order_by:
            col = order_by.get("column", "").lower()
            order = order_by.get("order", "asc")

            if col in df.columns:
                df = df.sort_values(by=col, ascending=(order == "asc"))

        if limit:
            df = df.head(limit)

    except Exception as e:
        print("ORDER ERROR:", e)

    return df.to_dict("records")


# =========================
# PAGINATION (NEW)
# =========================
def apply_pagination(data, page, page_size):
    total = len(data)
    start = (page - 1) * page_size
    end = start + page_size
    return data[start:end], total


# =========================
# GROUP + AGG
# =========================
def apply_groupby_aggregation(data, group_by, aggregations):
    df = pd.DataFrame(data)

    if df.empty:
        return []

    group_by = [g.lower() for g in group_by if g.lower() in df.columns]

    agg_map = {}

    for alias, config in aggregations.items():
        col = config["column"].lower()
        op = config["operation"]

        if col not in df.columns:
            continue

        if op == "sum":
            agg_map[alias] = (col, "sum")
        elif op == "avg":
            agg_map[alias] = (col, "mean")
        elif op == "count":
            agg_map[alias] = (col, "count")
        elif op == "max":
            agg_map[alias] = (col, "max")
        elif op == "min":
            agg_map[alias] = (col, "min")

    if group_by:
        df_grouped = df.groupby(group_by).agg(**agg_map).reset_index()
        return df_grouped.to_dict("records")
    else:
        result = {}
        for alias, (col, func) in agg_map.items():
            result[alias] = getattr(df[col], func)()
        return result


# =========================
# EXPLAINABILITY
# =========================
def build_explanation(sources, joins, aggregations, filters):
    return {
        "datasets_used": list(sources),
        "joins_applied": len(joins),
        "aggregations": list(aggregations.keys()),
        "filters_applied": filters
    }


# =========================
# PIPELINE
# =========================
async def run_pipeline(data):

    cache_key = make_cache_key(data)
    cached = get_cache(cache_key)
    if cached:
        return cached

    group = data.get("group1", {})

    columns = group.get("columnsToShow", [])
    table_selection = group.get("tableSelection", {})
    joins = group.get("joins", [])
    aggregations = group.get("aggregations", {})

    derived_fields = group.get("derivedFields", {})
    group_by = group.get("groupBy", [])

    filters = group.get("filters", [])
    order_by = group.get("orderBy", {})
    limit = group.get("limit", None)
    time_filter = group.get("timeFilter", {})

    page = group.get("page", 1)
    page_size = group.get("pageSize", 10)

    mysql_filters, mongo_filters, csv_filters, pdf_filters = split_filters(table_selection)

    needed_sources = detect_needed_sources(columns, joins, aggregations)

    tasks = {}

    if "mysql" in needed_sources:
        tasks["mysql"] = fetch_mysql(mysql_filters)
    if "mongo" in needed_sources:
        tasks["mongo"] = fetch_mongo(mongo_filters)
    if "csv" in needed_sources:
        tasks["csv"] = fetch_csv(csv_filters)
    if "pdf" in needed_sources:
        tasks["pdf"] = fetch_pdf(pdf_filters)

    results = await asyncio.gather(*tasks.values())
    data_sources = dict(zip(tasks.keys(), results))

    if joins:
        final_result = perform_joins(data_sources, joins)
    else:
        final_result = sum([v for v in data_sources.values() if isinstance(v, list)], [])

    final_result = normalize_columns(final_result)
    final_result = apply_derived_fields(final_result, derived_fields)

    final_result = apply_time_filter(final_result, time_filter)
    final_result = apply_advanced_filters(final_result, filters)

    if aggregations:
        final_result = apply_groupby_aggregation(final_result, group_by, aggregations)
    else:
        final_result = project_columns(final_result, columns)

    final_result = apply_order_limit(final_result, order_by, limit)

    paginated, total = apply_pagination(final_result, page, page_size)

    explanation = build_explanation(
        needed_sources,
        joins,
        aggregations,
        filters
    )

    final = {
        "answer": paginated,
        "total": total,
        "page": page,
        "pageSize": page_size,
        "explanation": explanation,
        "status": "success",
        "agent_used": "AI_DATA_ENGINE"
    }

    set_cache(cache_key, final)

    return final


# =========================
# MAIN API
# =========================
@router.post("/chat")
async def chat(data: dict = Body(...)):

    try:
        print("🔥 REQUEST:", data)

        if "group1" in data:
            return JSONResponse(content=await run_pipeline(data))

        question = data.get("question") or data.get("message")

        if not question:
            return {"error": "No question provided"}

        catalog = get_data_catalog()
        print("📊 CATALOG:", catalog)

        generated_json = generate_query_from_llm(question, catalog)

        print("🧠 LLM RAW:", generated_json)

        try:
            generated_json = json.loads(generated_json)
        except Exception:
            return {
                "error": "LLM returned invalid JSON",
                "raw": generated_json
            }

        result = await run_pipeline(generated_json)

        return JSONResponse(content=result)

    except Exception as e:
        import traceback
        traceback.print_exc()
        return {
            "error": "system failure",
            "details": str(e)
        }