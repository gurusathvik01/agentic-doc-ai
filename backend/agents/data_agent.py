import pandas as pd

def detect_columns(df):
    cols = df.columns

    # Detect name column
    name_col = next((c for c in cols if "name" in c.lower()), None)

    # Detect product column
    product_col = next((c for c in cols if "product" in c.lower()), None)

    # Detect value column
    value_col = next((c for c in cols if c.lower() in ["amount", "revenue", "sales", "profit"]), None)

    if value_col is None:
        numeric_cols = df.select_dtypes(include="number").columns
        value_col = numeric_cols[0] if len(numeric_cols) > 0 else None

    return name_col, product_col, value_col


def process_data(df):
    print("\n📊 Data Agent Output:\n")

    df = df.fillna(0)

    name_col, product_col, value_col = detect_columns(df)

    # Store detected columns for reuse
    df.attrs["name_col"] = name_col
    df.attrs["product_col"] = product_col
    df.attrs["value_col"] = value_col

    print("Detected:", name_col, product_col, value_col)
    print(df.head())

    return df