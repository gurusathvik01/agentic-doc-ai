import asyncio
import pandas as pd

from backend.db import query_mysql, query_mongo, query_csv
from backend.pdf_handler import query_pdf
from backend.source_map import SOURCE_MAP
from backend.filter_engine import (
    build_mysql_where,
    build_mongo_filter,
    apply_csv_filter,
    apply_pdf_filter
)


# =========================
# DETECT SOURCES
# =========================
def detect_sources(columns, filters):
    sources = set()

    for col in columns:
        if col in SOURCE_MAP:
            sources.add(SOURCE_MAP[col])

    for f in filters:
        col = f.get("column")
        if col in SOURCE_MAP:
            sources.add(SOURCE_MAP[col])

    return sources


# =========================
# FETCH DATA
# =========================
async def fetch_data(sources, filters):

    results = {}

    # MySQL
    if "mysql" in sources:
        where = build_mysql_where(filters)
        query = "SELECT * FROM Employees_Master"
        if where:
            query += f" WHERE {where}"

        results["mysql"] = query_mysql(query)

    # Mongo
    if "mongo" in sources:
        mongo_query = build_mongo_filter(filters)
        results["mongo"] = query_mongo("users_data", mongo_query)

    # CSV
    if "csv" in sources:
        df = pd.read_csv("backend/data/sales_extra.csv")
        df = apply_csv_filter(df, filters)
        results["csv"] = df.to_dict("records")

    # PDF
    if "pdf" in sources:
        data = query_pdf()
        data = apply_pdf_filter(data, filters)
        results["pdf"] = data

    return results


# =========================
# NORMALIZE DF
# =========================
def normalize_df(data):
    if not data:
        return pd.DataFrame()

    df = pd.DataFrame(data)
    df.columns = [c.lower().strip() for c in df.columns]
    return df


# =========================
# GET SOURCE FROM TABLE
# =========================
def get_source_from_table(table_name):
    from backend.data_catalog import get_data_catalog

    catalog = get_data_catalog()

    if table_name in catalog:
        return catalog[table_name]["source"]

    return None


# =========================
# 🔥 MULTI-SOURCE JOIN ENGINE
# =========================
def apply_multi_source_joins(data_sources, joins):

    dfs = {}

    # convert each source to df
    for source, data in data_sources.items():
        df = normalize_df(data)
        if not df.empty:
            dfs[source] = df

    # 🔥 NO JOIN → CONCAT
    if not joins:
        if dfs:
            combined = pd.concat(dfs.values(), ignore_index=True, sort=False)
            return combined.to_dict("records")
        return []

    result_df = None

    for join in joins:
        left = join["left"][0]
        right = join["right"][0]

        left_table, left_col = left.split(".")
        right_table, right_col = right.split(".")

        left_source = get_source_from_table(left_table)
        right_source = get_source_from_table(right_table)

        left_df = dfs.get(left_source)
        right_df = dfs.get(right_source)

        if left_df is None or right_df is None:
            continue

        if left_col not in left_df.columns or right_col not in right_df.columns:
            continue

        merged = left_df.merge(
            right_df,
            left_on=left_col,
            right_on=right_col,
            how="inner"
        )

        if result_df is None:
            result_df = merged
        else:
            result_df = result_df.merge(merged, how="inner")

    if result_df is not None:
        return result_df.to_dict("records")

    # fallback
    if dfs:
        combined = pd.concat(dfs.values(), ignore_index=True, sort=False)
        return combined.to_dict("records")

    return []


# =========================
# APPLY GROUP + AGG
# =========================
def apply_aggregation(data, group_by, aggregations):

    if not data:
        return data

    df = pd.DataFrame(data)

    if df.empty:
        return []

    agg_map = {}

    for alias, config in aggregations.items():
        col = config["column"]
        op = config["operation"]

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
        df = df.groupby(group_by).agg(**agg_map).reset_index()
    else:
        result = {}
        for alias, (col, func) in agg_map.items():
            result[alias] = getattr(df[col], func)()
        return [result]

    return df.to_dict("records")


# =========================
# APPLY ORDER + LIMIT
# =========================
def apply_order_limit(data, order_by, limit):

    if not data:
        return data

    df = pd.DataFrame(data)

    if order_by:
        col = order_by.get("column")
        order = order_by.get("order", "asc")

        if col in df.columns:
            df = df.sort_values(by=col, ascending=(order == "asc"))

    if limit:
        df = df.head(limit)

    return df.to_dict("records")


# =========================
# MAIN ORCHESTRATOR
# =========================
async def run_orchestrator(query):

    columns = query.get("columnsToShow", [])
    filters = query.get("filters", [])
    joins = query.get("joins", [])
    aggregations = query.get("aggregations", {})
    group_by = query.get("groupBy", [])
    order_by = query.get("orderBy", {})
    limit = query.get("limit", None)

    # 1️⃣ detect sources
    sources = detect_sources(columns, filters)

    # 2️⃣ fetch data
    data_sources = await fetch_data(sources, filters)

    # 🔥 3️⃣ MULTI-SOURCE JOIN (UPDATED)
    data = apply_multi_source_joins(data_sources, joins)

    # 4️⃣ aggregation
    if aggregations:
        data = apply_aggregation(data, group_by, aggregations)

    # 5️⃣ order + limit
    data = apply_order_limit(data, order_by, limit)

    return {
        "answer": data,
        "sources_used": list(sources),
        "status": "success"
    }