# ===================== IMPORTS =====================

from backend.query_planner import generate_query_from_llm
from backend.data_catalog import get_data_catalog, filter_catalog
from backend.routes.chat import run_pipeline
from backend.page_index import detect_sources   # 🔥 NEW

# agents
from backend.agents.data_agent import process_data
from backend.agents.insight_agent import generate_insights
from backend.agents.decision_agent import make_final_decision
from backend.agents.risk_agent import calculate_risk


# ==================================================
# 🔥 1. AGENT ROUTING
# ==================================================
def route_agent(query: str):
    q = query.lower()

    if "risk" in q:
        return "risk"
    elif "profit" in q or "sales" in q:
        return "data"
    elif "insight" in q or "analysis" in q:
        return "insight"
    else:
        return "decision"


# ==================================================
# 🔥 2. MAIN QUERY HANDLER (UPDATED)
# ==================================================
async def handle_query(query: str):

    # 🔹 Step 0: Detect sources (NEW 🔥)
    selected_sources = detect_sources(query)
    print("📌 Selected Sources:", selected_sources)

    # 🔹 Step 1: Get full catalog
    catalog = get_data_catalog()

    # 🔹 Step 2: Filter catalog based on detected sources (NEW 🔥)
    filtered_catalog = filter_catalog(catalog, selected_sources)

    # 🔹 Step 3: Generate structured query (using filtered catalog)
    structured_query = generate_query_from_llm(query, filtered_catalog)

    print("🧠 STRUCTURED QUERY:", structured_query)

    # 🔹 Step 4: Run your main engine
    result = await run_pipeline(structured_query)

    data = result.get("answer", [])

    # 🔹 Step 5: Choose agent
    agent = route_agent(query)

    processed = None
    insights = None
    risks = None
    decision = None

    # 🔹 Step 6: Apply agents (if data exists)
    if isinstance(data, list) and data:
        try:
            import pandas as pd

            df = pd.DataFrame(data)

            if agent == "data":
                processed = process_data(df)

            elif agent == "insight":
                processed = process_data(df)
                insights = generate_insights(processed)

            elif agent == "risk":
                processed = process_data(df)
                risks = calculate_risk(processed)

            else:
                processed = process_data(df)
                insights = generate_insights(processed)
                risks = calculate_risk(processed)
                decision = make_final_decision(processed, insights, risks)

        except Exception as e:
            print("Agent Error:", e)

    # 🔹 Step 7: Final response
    return {
        "answer": data,
        "processed": processed,
        "insights": insights,
        "risks": risks,
        "decision": decision,
        "agent_used": agent,
        "selected_sources": selected_sources,   # 🔥 NEW (for debugging/demo)
        "explanation": result.get("explanation", {}),
        "status": "success"
    }