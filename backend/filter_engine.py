# =========================
# 🔥 MULTI SOURCE FILTER ENGINE (FINAL)
# =========================

# =========================
# NORMALIZE FILTERS (LLM → ENGINE)
# =========================
def normalize_filters(filters):
    """
    Convert LLM filter format to engine format
    """
    normalized = {}

    if not isinstance(filters, list):
        return normalized

    op_map = {
        ">": "gt",
        "<": "lt",
        ">=": "gte",
        "<=": "lte",
        "=": "eq",
        "!=": "ne",
        "in": "in"
    }

    for f in filters:
        col = f.get("column", "").lower()
        op = f.get("op")
        val = f.get("value")

        if not col or not op:
            continue

        mapped_op = op_map.get(op)

        if not mapped_op:
            continue

        if mapped_op == "eq":
            normalized[col] = val
        elif mapped_op == "in":
            normalized[col] = val if isinstance(val, list) else [val]
        else:
            normalized.setdefault(col, {})
            normalized[col][mapped_op] = val

    return normalized


# =========================
# SAFE SQL VALUE
# =========================
def safe_sql_value(val):
    if isinstance(val, (int, float)):
        return str(val)
    return "'" + str(val).replace("'", "''") + "'"


# =========================
# MYSQL WHERE BUILDER
# =========================
def build_mysql_where(filters):
    filters = normalize_filters(filters)

    clauses = []

    for col, val in filters.items():

        if isinstance(val, list):
            vals = ", ".join([safe_sql_value(v) for v in val])
            clauses.append(f"{col} IN ({vals})")

        elif isinstance(val, dict):

            if "gt" in val:
                clauses.append(f"{col} > {safe_sql_value(val['gt'])}")

            if "lt" in val:
                clauses.append(f"{col} < {safe_sql_value(val['lt'])}")

            if "gte" in val:
                clauses.append(f"{col} >= {safe_sql_value(val['gte'])}")

            if "lte" in val:
                clauses.append(f"{col} <= {safe_sql_value(val['lte'])}")

            if "ne" in val:
                clauses.append(f"{col} != {safe_sql_value(val['ne'])}")

        else:
            clauses.append(f"{col} = {safe_sql_value(val)}")

    return " AND ".join(clauses)


# =========================
# MONGO FILTER
# =========================
def build_mongo_filter(filters):
    filters = normalize_filters(filters)

    query = {}

    for col, val in filters.items():

        if isinstance(val, list):
            query[col] = {"$in": val}

        elif isinstance(val, dict):
            mongo_ops = {}

            if "gt" in val:
                mongo_ops["$gt"] = val["gt"]

            if "lt" in val:
                mongo_ops["$lt"] = val["lt"]

            if "gte" in val:
                mongo_ops["$gte"] = val["gte"]

            if "lte" in val:
                mongo_ops["$lte"] = val["lte"]

            if "ne" in val:
                mongo_ops["$ne"] = val["ne"]

            if mongo_ops:
                query[col] = mongo_ops

        else:
            query[col] = val

    return query


# =========================
# CSV FILTER
# =========================
def apply_csv_filter(df, filters):
    import pandas as pd

    filters = normalize_filters(filters)

    df.columns = [c.lower() for c in df.columns]

    for col, val in filters.items():

        if col not in df.columns:
            continue

        if isinstance(val, list):
            df = df[df[col].isin(val)]

        elif isinstance(val, dict):

            if "gt" in val:
                df = df[df[col] > val["gt"]]

            if "lt" in val:
                df = df[df[col] < val["lt"]]

            if "gte" in val:
                df = df[df[col] >= val["gte"]]

            if "lte" in val:
                df = df[df[col] <= val["lte"]]

            if "ne" in val:
                df = df[df[col] != val["ne"]]

        else:
            df = df[df[col] == val]

    return df


# =========================
# PDF FILTER
# =========================
def apply_pdf_filter(data, filters):
    filters = normalize_filters(filters)

    result = []

    for row in data:
        match = True

        for col, val in filters.items():
            row_val = str(row.get(col, "")).strip().lower()

            if isinstance(val, list):
                if row_val not in [str(v).lower() for v in val]:
                    match = False
                    break

            elif isinstance(val, dict):
                if "ne" in val and row_val == str(val["ne"]).lower():
                    match = False
                    break

            else:
                if row_val != str(val).lower():
                    match = False
                    break

        if match:
            result.append(row)

    return result