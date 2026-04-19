def calculate_risk(df):
    print("\n⚠️ Risk Agent Output:\n")

    risk_data = []

    name_col = df.attrs.get("name_col")
    product_col = df.attrs.get("product_col")
    value_col = df.attrs.get("value_col")

    for _, row in df.iterrows():

        value = row[value_col] if value_col else 0

        if value > 70000:
            risk = "High"
        elif value > 30000:
            risk = "Medium"
        else:
            risk = "Low"

        risk_data.append({
            "customer": row[name_col] if name_col else "Unknown",
            "product": row[product_col] if product_col else "Unknown",
            "risk": risk
        })

    print("Risk:", risk_data[:5])

    return risk_data