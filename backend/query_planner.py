from backend.llm_engine import ask_llm
import json
import re


# =========================
# STRONG JSON PARSER
# =========================
def safe_parse_json(text):
    try:
        return json.loads(text)
    except:
        try:
            match = re.search(r"\{.*\}", text, re.DOTALL)
            if match:
                return json.loads(match.group(0))
        except:
            pass
    return None


# =========================
# DEFAULT FALLBACK
# =========================
def fallback_query():
    return {
        "columnsToShow": ["empcode", "firstname"],
        "tableSelection": {},
        "joins": [],
        "aggregations": {},
        "groupBy": [],
        "filters": [],
        "orderBy": {},
        "limit": None,
        "page": 1,
        "pageSize": 10
    }


# =========================
# 🔥 FIX JOINS FORMAT
# =========================
def fix_joins_format(query):
    joins = query.get("joins", [])

    fixed = []

    for j in joins:

        # correct format
        if "left" in j and "right" in j:
            fixed.append(j)
            continue

        # fix old format
        if all(k in j for k in ["table", "column", "relatedTable", "relatedColumn"]):
            fixed.append({
                "left": [f"{j['table']}.{j['column']}"],
                "right": [f"{j['relatedTable']}.{j['relatedColumn']}"]
            })

    query["joins"] = fixed
    return query


# =========================
# 🔥 VALIDATE + CLEAN QUERY
# =========================
def normalize_query(query, catalog):

    # required keys
    default = fallback_query()

    for key in default:
        if key not in query:
            query[key] = default[key]

    # lowercase columns
    query["columnsToShow"] = [c.lower() for c in query.get("columnsToShow", [])]
    query["groupBy"] = [c.lower() for c in query.get("groupBy", [])]

    # fix joins
    query = fix_joins_format(query)

    # 🔥 FIX aggregation + groupby mismatch
    if query["aggregations"] and not query["groupBy"]:
        # auto add groupBy = columnsToShow
        query["groupBy"] = query["columnsToShow"]

    # 🔥 REMOVE INVALID COLUMNS
    valid_columns = set()

    for table in catalog.values():
        valid_columns.update(table.get("columns", []))

    query["columnsToShow"] = [
        c for c in query["columnsToShow"] if c in valid_columns
    ]

    return query


# =========================
# MAIN FUNCTION
# =========================
def generate_query_from_llm(user_query, catalog):

    prompt = f"""
You are a STRICT JSON query planner.

AVAILABLE DATASETS:
{catalog}

RELATIONSHIPS:
- employees_master.empcode = dialers_staging.agent_id
- employees_master.unitcode = unit_master.unitcode

OUTPUT JSON FORMAT:

{{
  "columnsToShow": [],
  "tableSelection": {{}},
  "joins": [],
  "aggregations": {{}},
  "groupBy": [],
  "filters": [],
  "orderBy": {{}},
  "limit": null,
  "page": 1,
  "pageSize": 10
}}

RULES:
- ONLY JSON
- NO explanation
- lowercase columns only
- valid columns only
- joins must be:
  {{
    "left": ["table.column"],
    "right": ["table.column"]
  }}

USER QUERY:
{user_query}
"""

    # 🔥 LLM CALL
    raw_output = ask_llm(prompt)

    print("\n🧠 LLM RAW OUTPUT:\n", raw_output)

    parsed = safe_parse_json(raw_output)

    if not parsed:
        print("⚠️ LLM failed → using fallback")
        return fallback_query()

    # 🔥 FINAL CLEAN + FIX
    parsed = normalize_query(parsed, catalog)

    print("\n✅ FINAL STRUCTURED QUERY:\n", parsed)

    return parsed