# =========================
# SOURCE MAP (PRODUCTION SAFE)
# =========================

SOURCE_MAP = {

    # =========================
    # MYSQL (Employees_Master)
    # =========================
    "empcode": "mysql",
    "emp_code": "mysql",

    "firstname": "mysql",
    "first_name": "mysql",

    "functionalmanager": "mysql",
    "functional_manager": "mysql",

    "unitcode": "mysql",
    "unit_code": "mysql",

    # =========================
    # MYSQL (JOIN TABLES - IMPORTANT)
    # =========================
    "agent_id": "mysql",
    "start_time": "mysql",
    "end_time": "mysql",

    "level_name": "mysql",
    "location_name": "mysql",

    # =========================
    # CSV (sales_extra.csv)
    # =========================
    "product": "csv",
    "revenue": "csv",
    "cost": "csv",
    "profit": "csv",

    # variations (LLM safety)
    "Product": "csv",
    "Revenue": "csv",
    "Cost": "csv",
    "Profit": "csv",

    # =========================
    # MONGO
    # =========================
    "userid": "mongo",
    "user_id": "mongo",

    "username": "mongo",
    "user_name": "mongo",

    "email": "mongo",

    # =========================
    # PDF
    # =========================
    "pdf_summary": "pdf",
    "summary": "pdf",

    # =========================
    # DERIVED / CALCULATED (SAFE FALLBACK)
    # =========================
    "working_hours": "derived",
    "attendance": "derived"
}