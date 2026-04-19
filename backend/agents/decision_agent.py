def make_final_decision(df, insights, risks):

    name_col = df.attrs.get("name_col")
    product_col = df.attrs.get("product_col")
    value_col = df.attrs.get("value_col")

    total_value = float(df[value_col].sum()) if value_col else 0

    return {
        "report": {
            "title": "AI Sales Report",

            "kpis": {
                "total_sales": total_value,
                "top_customer": insights.get("top_customer", {}).get(name_col, "N/A"),
                "top_product": insights.get("most_sold_product", {}).get("product", "N/A")
            },

            "charts": [
                {
                    "type": "bar",
                    "x": df[product_col].tolist() if product_col else [],
                    "y": df[value_col].tolist() if value_col else [],
                    "title": "Sales by Product"
                }
            ],

            "insights": insights,
            "risks": risks[:5],

            "summary": "AI-generated decision based on dataset."
        }
    }