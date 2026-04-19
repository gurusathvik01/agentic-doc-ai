from backend.db import query_mongo
import pandas as pd
import os


# =========================
# NORMALIZATION
# =========================
def normalize_column(col):
    return col.lower().strip()


# =========================
# MYSQL SCHEMA (MANUAL MAP)
# =========================
def get_mysql_catalog():
    return {
        "employees_master": {
            "columns": [
                "empcode",
                "firstname",
                "functionalmanager",
                "unitcode"
            ],
            "source": "mysql"
        },
        "dialers_staging": {
            "columns": [
                "agent_id",
                "start_time",
                "end_time"
            ],
            "source": "mysql"
        },
        "unit_master": {
            "columns": [
                "unitcode",
                "location_name"
            ],
            "source": "mysql"
        }
    }


# =========================
# CSV SCHEMA (AUTO LOAD)
# =========================
def get_csv_catalog():
    catalog = {}

    try:
        base_path = os.path.join("backend", "data")

        for file in os.listdir(base_path):
            if file.endswith(".csv"):
                name = file.replace(".csv", "")
                file_path = os.path.join(base_path, file)

                df = pd.read_csv(file_path)

                columns = [normalize_column(col) for col in df.columns]

                catalog[name] = {
                    "columns": columns,
                    "source": "csv"
                }

    except Exception as e:
        print("❌ CSV Catalog Error:", e)

    return catalog


# =========================
# MONGO SCHEMA
# =========================
def get_mongo_catalog():
    catalog = {}

    try:
        docs = query_mongo("global_index", {})

        for doc in docs:
            name = doc.get("name")
            columns = doc.get("columns", [])

            if name and columns:
                normalized_cols = [normalize_column(col) for col in columns]

                catalog[name] = {
                    "columns": normalized_cols,
                    "source": "mongo"
                }

    except Exception as e:
        print("❌ Mongo Catalog Error:", e)

    return catalog


# =========================
# FINAL DATA CATALOG
# =========================
def get_data_catalog():

    catalog = {}

    # MYSQL
    catalog.update(get_mysql_catalog())

    # MONGO
    catalog.update(get_mongo_catalog())

    # CSV
    catalog.update(get_csv_catalog())

    print("📊 FINAL CATALOG:", catalog)

    return catalog


# =========================
# 🔥 FILTER CATALOG (FOR PAGE INDEX)
# =========================
def filter_catalog(catalog, selected_sources):
    """
    Keep only datasets selected by page_index
    """
    return {
        name: schema
        for name, schema in catalog.items()
        if name in selected_sources
    }