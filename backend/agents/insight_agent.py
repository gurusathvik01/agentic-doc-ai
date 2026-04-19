def generate_insights(df):
    print("\n🧠 Insight Agent Output:\n")

    insights = {}

    name_col = df.attrs.get("name_col")
    product_col = df.attrs.get("product_col")
    value_col = df.attrs.get("value_col")

    # ✅ Top customer
    if name_col and value_col:
        top_customer = (
            df.groupby(name_col)[value_col]
            .sum()
            .sort_values(ascending=False)
            .reset_index()
        )
        insights["top_customer"] = top_customer.iloc[0].to_dict()
    else:
        insights["top_customer"] = {}

    # ✅ Most sold product
    if product_col:
        most_sold = df[product_col].value_counts().reset_index()
        most_sold.columns = ["product", "count"]
        insights["most_sold_product"] = most_sold.iloc[0].to_dict()
    else:
        insights["most_sold_product"] = {}

    print("Insights:", insights)

    return insights