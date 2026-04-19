@echo off
REM Agentic Doc AI v2 Frontend Startup

echo Starting Agentic Doc AI v2 Frontend...

REM Activate virtual environment
call venv_v2\Scripts\activate

REM Start frontend
cd frontend
streamlit run app_v2.py --server.port=8501 --server.address=0.0.0.0

echo Frontend started on http://localhost:8501
pause
