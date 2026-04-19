import streamlit as st
import pandas as pd
import requests
import uuid
import json
import plotly.express as px
import time
import hashlib




# BACKEND IMPORTS
from backend.mineru_extractor import extract_table_from_file
from backend.agents.data_agent import process_data
from backend.agents.insight_agent import generate_insights
from backend.agents.decision_agent import make_final_decision
from backend.agents.risk_agent import calculate_risk

from backend.memory import save_to_memory, save_chat
from backend.db import save_to_mongo, save_to_mysql, fetch_from_mongo, fetch_from_mysql

API_URL = "http://127.0.0.1:8000"

# ================= CONFIG =================
st.set_page_config(page_title="Agentic AI Pro", layout="wide")

st.title("🤖 Agentic AI System")
st.caption("AI Agent that finds where your data is, fetches it, and answers intelligently")

# ================= SESSION =================
# ================= SESSION =================
if "chats" not in st.session_state:
    st.session_state.chats = {}

if "current_chat" not in st.session_state:
    st.session_state.current_chat = "default"

# ✅ Ensure default chat has name + messages
if "default" not in st.session_state.chats:
    st.session_state.chats["default"] = {
        "name": "Default Chat",
        "messages": []
    }

# 🔥 Fix old chats (migration safety)
for chat_id, chat in st.session_state.chats.items():
    if "name" not in chat:
        chat["name"] = f"Chat {chat_id[:4]}"
    if "messages" not in chat:
        chat["messages"] = []

# ================= DATA SESSION =================
if "indexed_hashes" not in st.session_state:
    st.session_state.indexed_hashes = set()

if "loaded_sources" not in st.session_state:
    st.session_state.loaded_sources = set()

if "df_store" not in st.session_state:
    st.session_state.df_store = []

# ================= HELPERS =================
def get_hash(text):
    return hashlib.md5(text.encode()).hexdigest()

def add_to_rag_safe(text):
    h = get_hash(text)
    if h not in st.session_state.indexed_hashes:
        try:
            requests.post(f"{API_URL}/add-data", json={"text": text}, timeout=2)
            st.session_state.indexed_hashes.add(h)
        except Exception as e:
            print("RAG ERROR:", e)

# ================= SIDEBAR =================
st.sidebar.title("🧠 Agent Control Panel")

# CHAT
st.sidebar.subheader("💬 Chats")

if st.sidebar.button("➕ New Chat", key="new_chat_btn"):
    chat_id = str(uuid.uuid4())
    st.session_state.chats[chat_id] = {
    "name": f"Chat {len(st.session_state.chats) + 1}",
    "messages": []
}
    st.session_state.current_chat = chat_id

for chat_id, chat in list(st.session_state.chats.items()):
    chat_name = chat.get("name", f"Chat {chat_id[:4]}")
    if st.sidebar.button(chat_name):
        st.session_state.current_chat = chat_id
        st.rerun()
# RESET
st.sidebar.markdown("---")
if st.sidebar.button("🔄 Reset Index", key="reset_btn"):
    requests.post(f"{API_URL}/reset-rag")
    st.session_state.indexed_hashes.clear()
    st.session_state.loaded_sources.clear()
    st.session_state.df_store.clear()
    st.success("Index reset ✅")

# SHOW SOURCES
st.sidebar.markdown("### 📦 Loaded Sources")
for src in st.session_state.loaded_sources:
    st.sidebar.markdown(f"• {src}")

# ================= DATA SOURCES =================
st.sidebar.markdown("---")
st.sidebar.header("📂 Data Sources")

df = None

# ================= MULTI FILE UPLOAD =================
files = st.sidebar.file_uploader("Upload CSV / Excel", accept_multiple_files=True)

if files:
    count = 0
    for file in files:
        temp_df = extract_table_from_file(file)

        if temp_df is not None and not temp_df.empty:
            st.session_state.df_store.append(temp_df)
            st.session_state.loaded_sources.add(f"Upload: {file.name}")

            for _, row in temp_df.iterrows():
                add_to_rag_safe(f"source:Upload:{file.name} | {row.to_dict()}")

            count += 1

    st.sidebar.success(f"{count} file(s) indexed ✅")

# ================= MONGO =================
st.sidebar.markdown("### 🍃 MongoDB")
col = st.sidebar.text_input("Collection Name", key="mongo_input")

if st.sidebar.button("Fetch Mongo", key="mongo_btn"):
    df_mongo = fetch_from_mongo(col)

    if df_mongo is not None and not df_mongo.empty:
        st.session_state.df_store.append(df_mongo)
        st.session_state.loaded_sources.add(f"MongoDB: {col}")

        for _, row in df_mongo.iterrows():
            add_to_rag_safe(f"source:MongoDB:{col} | {row.to_dict()}")

        st.sidebar.success("Mongo indexed ✅")
    else:
        st.sidebar.warning("No data found")

# ================= MYSQL =================
st.sidebar.markdown("### 🛢️ MySQL")
table_name = st.sidebar.text_input("Table Name", value="sales", key="mysql_input")

if st.sidebar.button("Fetch MySQL", key="mysql_btn"):
    df_mysql = fetch_from_mysql(table_name)

    if df_mysql is None or df_mysql.empty:
        st.warning("No data found")
    else:
        st.success("MySQL loaded ✅")
        st.dataframe(df_mysql)

        st.session_state.df_store.append(df_mysql)
        st.session_state.loaded_sources.add(f"MySQL: {table_name}")

        for _, row in df_mysql.iterrows():
            add_to_rag_safe(f"source:MySQL:{table_name} | {row.to_dict()}")

# ================= API =================
st.sidebar.markdown("### 🌐 API")
url = st.sidebar.text_input("API URL", key="api_input")

if st.sidebar.button("Fetch API", key="api_btn"):
    try:
        res = requests.get(url)
        df_api = pd.DataFrame(res.json())

        st.session_state.df_store.append(df_api)
        st.session_state.loaded_sources.add("API")

        st.sidebar.success("API data loaded ✅")
    except Exception as e:
        st.sidebar.error(str(e))

# ================= MANUAL =================
st.sidebar.markdown("### ✍️ Manual Input")
txt = st.sidebar.text_area("Paste CSV", key="manual_input")

if st.sidebar.button("Convert Manual", key="manual_btn"):
    df_manual = pd.DataFrame([x.split(",") for x in txt.split("\n")])
    st.session_state.df_store.append(df_manual)
    st.session_state.loaded_sources.add("Manual")
    st.sidebar.success("Manual data loaded ✅")

# ================= MERGE =================
if st.session_state.df_store:
    try:
        df = pd.concat(st.session_state.df_store, ignore_index=True)
    except Exception as e:
        st.error(f"Data merge error: {e}")
        df = None

# ================= PROCESS =================
if df is not None:
    try:
        with st.spinner("⚙️ Agent processing data..."):
            df = process_data(df)
            insights = generate_insights(df)
            risks = calculate_risk(df)
            decision = make_final_decision(df, insights, risks)

        save_to_mongo("processed_data", df)
        save_to_mysql("processed_data", df)

        st.success("✅ Data processed")

        col1, col2, col3 = st.columns(3)
        col1.metric("Rows", len(df))
        col2.metric("Columns", len(df.columns))
        col3.metric("Profit", df["profit"].sum() if "profit" in df.columns else 0)

        if "product" in df.columns and "profit" in df.columns:
            fig = px.bar(df, x="product", y="profit")
            st.plotly_chart(fig, use_container_width=True)

        st.dataframe(df)

        st.download_button(
            "⬇ Download AI Report",
            data=json.dumps(decision, indent=2),
            file_name="report.json"
        )

    except Exception as e:
        st.error(str(e))


# ================= CHAT =================
st.markdown("---")

if not st.session_state.current_chat:
    st.info("👈 Create a new chat")
    st.stop()

chat = st.session_state.chats.get(st.session_state.current_chat, {"messages": []})

if chat is None:
    st.warning("⚠️ Chat not found. Create a new chat.")
    st.stop()

# SHOW OLD MESSAGES
for msg in chat.get("messages", []):
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])
# INPUT
user_input = st.chat_input("Ask anything about your data...", key="main_chat_input")

if user_input:
    chat["messages"].append({"role": "user", "content": user_input})

    with st.chat_message("assistant"):
        thinking = st.empty()

        # 🔥 Smooth thinking animation
        thinking.info("🧠 Understanding query...")
        time.sleep(0.2)

        thinking.info("🔎 Searching datasets...")
        time.sleep(0.2)

        thinking.info("📦 Fetching data...")
        time.sleep(0.2)

        try:
            response = requests.post(
                f"{API_URL}/chat",
                json={"question": user_input},
                timeout=60
            )

            # ❌ If backend error
            if response.status_code != 200:
                full_response = f"❌ Backend error: {response.status_code}"

            else:
                try:
                    # ✅ Try parsing JSON safely
                    data = response.json()

                    if isinstance(data, dict):
                        full_response = data.get("answer", str(data))

                        # Optional info display
                        if "sources_used" in data:
                            st.caption(f"🧠 Sources: {data['sources_used']}")

                        if "agent_used" in data:
                            st.caption(f"🤖 Agent: {data['agent_used']}")
                    else:
                        # fallback if weird format
                        full_response = str(data)

                except Exception as e:
                    # 🔥 FINAL FIX: fallback instead of error
                    print("JSON ERROR:", e)
                    print("RAW RESPONSE:", response.text)

                    full_response = response.text  # ✅ USE RAW TEXT

        except Exception as e:
            full_response = f"❌ Error: {str(e)}"

        # 🔥 Clean output
        thinking.empty()
        st.markdown(full_response)

        chat["messages"].append({
            "role": "assistant",
            "content": full_response
        })