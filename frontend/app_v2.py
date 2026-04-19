import streamlit as st
import requests
import json
import time
import pandas as pd
from datetime import datetime
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import asyncio
import uuid
from typing import Dict, List, Any

# Configuration
API_URL = "http://127.0.0.1:8000"

# Page configuration
st.set_page_config(
    page_title="Agentic Doc AI v2",
    layout="wide",
    initial_sidebar_state="expanded",
    page_icon="🤖"
)

# Custom CSS for modern UI
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: 700;
        color: #1f2937;
        margin-bottom: 1rem;
    }
    
    .metric-card {
        background: white;
        padding: 1rem;
        border-radius: 0.5rem;
        box-shadow: 0 1px 3px rgba(0,0,0,0.12);
        border-left: 4px solid #3b82f6;
    }
    
    .query-input {
        font-size: 1.1rem;
        padding: 1rem;
        border-radius: 0.5rem;
        border: 2px solid #e5e7eb;
    }
    
    .response-container {
        background: #f9fafb;
        padding: 1.5rem;
        border-radius: 0.5rem;
        border-left: 4px solid #10b981;
        margin: 1rem 0;
    }
    
    .source-badge {
        display: inline-block;
        padding: 0.25rem 0.75rem;
        background: #dbeafe;
        color: #1e40af;
        border-radius: 9999px;
        font-size: 0.875rem;
        margin: 0.25rem;
    }
    
    .processing-step {
        background: #fef3c7;
        padding: 0.75rem;
        border-radius: 0.375rem;
        margin: 0.5rem 0;
        border-left: 3px solid #f59e0b;
    }
    
    .status-indicator {
        display: inline-block;
        width: 8px;
        height: 8px;
        border-radius: 50%;
        margin-right: 0.5rem;
    }
    
    .status-success { background: #10b981; }
    .status-error { background: #ef4444; }
    .status-processing { background: #f59e0b; }
    
    .report-section {
        background: white;
        padding: 1.5rem;
        border-radius: 0.5rem;
        margin: 1rem 0;
        box-shadow: 0 1px 3px rgba(0,0,0,0.12);
    }
</style>
""", unsafe_allow_html=True)

# Initialize session state
def init_session_state():
    if 'query_history' not in st.session_state:
        st.session_state.query_history = []
    if 'current_query' not in st.session_state:
        st.session_state.current_query = ""
    if 'processing' not in st.session_state:
        st.session_state.processing = False
    if 'system_status' not in st.session_state:
        st.session_state.system_status = {}
    if 'available_sources' not in st.session_state:
        st.session_state.available_sources = {}

init_session_state()

# Helper functions
def get_system_status():
    """Get system status from API"""
    try:
        response = requests.get(f"{API_URL}/system-status", timeout=5)
        if response.status_code == 200:
            return response.json()
    except:
        pass
    return {"success": False, "status": {}}

def get_available_sources():
    """Get available data sources"""
    try:
        response = requests.get(f"{API_URL}/sources", timeout=5)
        if response.status_code == 200:
            return response.json()
    except:
        pass
    return {"success": False, "sources": {}}

def process_query(query: str) -> Dict[str, Any]:
    """Process a query through the API"""
    try:
        response = requests.post(
            f"{API_URL}/query",
            json={"query": query},
            timeout=120
        )
        
        if response.status_code == 200:
            return response.json()
        else:
            return {
                "success": False,
                "error": f"API Error: {response.status_code}",
                "error_message": response.text
            }
    except Exception as e:
        return {
            "success": False,
            "error": "Connection Error",
            "error_message": str(e)
        }

def upload_and_index_file(uploaded_file) -> Dict[str, Any]:
    """Upload and index a file"""
    try:
        files = {"file": uploaded_file}
        response = requests.post(f"{API_URL}/upload-and-index", files=files, timeout=60)
        
        if response.status_code == 200:
            return response.json()
        else:
            return {
                "success": False,
                "error": f"Upload Error: {response.status_code}",
                "error_message": response.text
            }
    except Exception as e:
        return {
            "success": False,
            "error": "Upload Failed",
            "error_message": str(e)
        }

def get_query_history():
    """Get query history from API"""
    try:
        response = requests.get(f"{API_URL}/query-history?limit=20", timeout=5)
        if response.status_code == 200:
            return response.json()
    except:
        pass
    return {"success": False, "history": []}

# Main UI
def main():
    # Header
    st.markdown('<h1 class="main-header">🤖 Agentic Doc AI v2</h1>', unsafe_allow_html=True)
    st.markdown("Production-ready AI system for multi-dataset querying with PageIndex integration")
    
    # System status bar
    with st.container():
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            if st.button("🔄 Refresh Status", key="refresh_status"):
                st.session_state.system_status = get_system_status()
                st.session_state.available_sources = get_available_sources()
                st.rerun()
        
        # Display system status
        if not st.session_state.system_status:
            st.session_state.system_status = get_system_status()
            st.session_state.available_sources = get_available_sources()
        
        status = st.session_state.system_status.get("status", {})
        
        with col2:
            llm_status = status.get("llm_engine", "unknown")
            status_color = "status-success" if llm_status == "operational" else "status-error"
            st.markdown(f'<span class="status-indicator {status_color}"></span>LLM: {llm_status}', 
                       unsafe_allow_html=True)
        
        with col3:
            sources = st.session_state.available_sources.get("sources", {})
            mysql_status = sources.get("mysql", {}).get("available", False)
            mongo_status = sources.get("mongodb", {}).get("available", False)
            
            db_status = "✅" if (mysql_status or mongo_status) else "❌"
            st.markdown(f'<span class="status-indicator {"status-success" if (mysql_status or mongo_status) else "status-error"}"></span>DB: {db_status}', 
                       unsafe_allow_html=True)
        
        with col4:
            total_queries = status.get("total_queries_processed", 0)
            st.metric("Queries Processed", total_queries)
    
    st.markdown("---")
    
    # Sidebar
    with st.sidebar:
        st.header("🛠️ Control Panel")
        
        # File upload section
        st.subheader("📄 Document Indexing")
        uploaded_file = st.file_uploader(
            "Upload PDF or Markdown",
            type=['pdf', 'md', 'markdown'],
            help="Upload documents to index with PageIndex"
        )
        
        if uploaded_file and st.button("📤 Upload & Index", key="upload_btn"):
            with st.spinner("Indexing document..."):
                result = upload_and_index_file(uploaded_file)
                if result["success"]:
                    st.success(f"✅ {result.get('message', 'Document indexed successfully')}")
                    st.json({
                        "Document ID": result.get("doc_id"),
                        "File": result.get("filename"),
                        "Type": result.get("doc_type")
                    })
                else:
                    st.error(f"❌ {result.get('error', 'Upload failed')}")
                    if result.get("error_message"):
                        st.code(result["error_message"])
        
        st.markdown("---")
        
        # Data sources info
        st.subheader("📊 Data Sources")
        sources = st.session_state.available_sources.get("sources", {})
        
        if sources.get("mysql", {}).get("available"):
            mysql_info = sources["mysql"]
            st.success("🛢️ MySQL Connected")
            st.write(f"Tables: {len(mysql_info.get('tables', []))}")
            if mysql_info.get("tables"):
                st.write("Available tables:", ", ".join(mysql_info["tables"][:5]))
        
        if sources.get("mongodb", {}).get("available"):
            mongo_info = sources["mongodb"]
            st.success("🍃 MongoDB Connected")
            st.write(f"Collections: {len(mongo_info.get('collections', []))}")
            if mongo_info.get("collections"):
                st.write("Available collections:", ", ".join(mongo_info["collections"][:5]))
        
        st.markdown("---")
        
        # Query history
        st.subheader("📜 Recent Queries")
        if st.button("🔄 Refresh History", key="refresh_history"):
            history_data = get_query_history()
            if history_data["success"]:
                st.session_state.query_history = history_data.get("history", [])
                st.rerun()
        
        # Display recent queries
        if st.session_state.query_history:
            for i, query_item in enumerate(st.session_state.query_history[-5:]):
                query_text = query_item.get("query", "Unknown query")[:50]
                if len(query_item.get("query", "")) > 50:
                    query_text += "..."
                
                if st.button(f"🔍 {query_text}", key=f"history_{i}"):
                    st.session_state.current_query = query_item.get("query", "")
                    st.rerun()
    
    # Main content area
    col1, col2 = st.columns([3, 1])
    
    with col1:
        # Query input
        st.subheader("💬 Ask Your Question")
        
        query = st.text_area(
            "Enter your query about any dataset:",
            value=st.session_state.current_query,
            placeholder="e.g., 'What are the sales trends for product X in the last quarter?'",
            height=100,
            key="query_input"
        )
        
        col_submit, col_clear = st.columns([1, 1])
        
        with col_submit:
            if st.button("🚀 Process Query", type="primary", disabled=st.session_state.processing):
                if query.strip():
                    st.session_state.processing = True
                    st.session_state.current_query = query
                    
                    # Process query
                    with st.spinner("🤖 Processing your query..."):
                        result = process_query(query)
                        
                        if result["success"]:
                            # Add to history
                            st.session_state.query_history.append(result)
                            
                            # Store result for display
                            st.session_state.last_result = result
                            st.success("✅ Query processed successfully!")
                        else:
                            st.error(f"❌ {result.get('error', 'Query failed')}")
                            if result.get("error_message"):
                                st.code(result["error_message"])
                    
                    st.session_state.processing = False
                    st.rerun()
        
        with col_clear:
            if st.button("🗑️ Clear"):
                st.session_state.current_query = ""
                st.session_state.last_result = None
                st.rerun()
        
        # Display results
        if hasattr(st.session_state, 'last_result') and st.session_state.last_result:
            result = st.session_state.last_result
            
            st.markdown("---")
            st.subheader("📋 Query Results")
            
            # Query metadata
            col_meta1, col_meta2, col_meta3 = st.columns(3)
            
            with col_meta1:
                st.metric("Processing Time", f"{result.get('processing_time', 0):.2f}s")
            
            with col_meta2:
                st.metric("Sources Used", len(result.get('sources_used', [])))
            
            with col_meta3:
                data_points = result.get('data_summary', {}).get('total_records', 0)
                st.metric("Data Points", data_points)
            
            # Sources used
            if result.get('sources_used'):
                st.markdown("**📊 Data Sources Used:**")
                for source in result['sources_used']:
                    st.markdown(f'<span class="source-badge">{source.upper()}</span>', 
                               unsafe_allow_html=True)
            
            # Final response
            st.markdown("### 🤖 AI Response")
            st.markdown(f'<div class="response-container">{result.get("final_response", "")}</div>', 
                       unsafe_allow_html=True)
            
            # Execution plan (expandable)
            if result.get('execution_plan'):
                with st.expander("🔧 Execution Plan"):
                    plan = result['execution_plan']
                    st.json({
                        "Plan ID": plan.get('plan_id'),
                        "Complexity": plan.get('estimated_complexity'),
                        "Merge Strategy": plan.get('merge_strategy'),
                        "Steps Count": len(plan.get('execution_steps', []))
                    })
                    
                    # Show execution steps
                    if plan.get('execution_steps'):
                        st.markdown("**Execution Steps:**")
                        for step in plan['execution_steps']:
                            step_status = "✅" if step.get('status') == 'completed' else "⏳"
                            st.markdown(f"{step_status} **{step.get('step_name', 'Unknown Step')}**")
                            st.write(f"Type: {step.get('step_type', 'unknown')}")
                            st.write(f"Description: {step.get('description', 'No description')}")
                            st.markdown("---")
            
            # Download report
            if st.button("📥 Download JSON Report", key="download_report"):
                report_data = {
                    "query_info": {
                        "query": result.get('query'),
                        "timestamp": result.get('timestamp'),
                        "processing_time": result.get('processing_time')
                    },
                    "intent": result.get('intent'),
                    "sources_used": result.get('sources_used'),
                    "data_summary": result.get('data_summary'),
                    "execution_plan": result.get('execution_plan'),
                    "final_response": result.get('final_response'),
                    "generated_at": datetime.now().isoformat()
                }
                
                st.download_button(
                    label="💾 Download Report",
                    data=json.dumps(report_data, indent=2),
                    file_name=f"agentic_report_{result.get('query_id', 'unknown')}.json",
                    mime="application/json"
                )
    
    with col2:
        # Analytics panel
        st.subheader("📈 Analytics")
        
        # System metrics
        status = st.session_state.system_status.get("status", {})
        
        st.markdown("### System Performance")
        col_perf1, col_perf2 = st.columns(2)
        
        with col_perf1:
            st.metric("Cache Size", status.get("cache_size", 0))
        
        with col_perf2:
            st.metric("Total Queries", status.get("total_queries_processed", 0))
        
        # Query performance chart (if we have history)
        if st.session_state.query_history:
            st.markdown("### Recent Performance")
            
            # Prepare data for chart
            recent_queries = st.session_state.query_history[-10:]
            processing_times = [q.get('processing_time', 0) for q in recent_queries]
            query_numbers = list(range(1, len(recent_queries) + 1))
            
            fig = px.line(
                x=query_numbers,
                y=processing_times,
                title="Processing Time Trend",
                labels={"x": "Query Number", "y": "Time (seconds)"}
            )
            fig.update_traces(line_color="#3b82f6")
            fig.update_layout(height=200)
            st.plotly_chart(fig, use_container_width=True)
        
        # Data sources breakdown
        if st.session_state.query_history:
            st.markdown("### Source Usage")
            
            source_counts = {}
            for query_item in st.session_state.query_history:
                for source in query_item.get('sources_used', []):
                    source_counts[source] = source_counts.get(source, 0) + 1
            
            if source_counts:
                fig = px.pie(
                    values=list(source_counts.values()),
                    names=list(source_counts.keys()),
                    title="Data Sources Usage"
                )
                fig.update_layout(height=200)
                st.plotly_chart(fig, use_container_width=True)

# Footer
st.markdown("---")
st.markdown(
    "<div style='text-align: center; color: #6b7280; font-size: 0.875rem;'>"
    "Agentic Doc AI v2 • Production-ready Multi-dataset Query System"
    "</div>",
    unsafe_allow_html=True
)

if __name__ == "__main__":
    main()
