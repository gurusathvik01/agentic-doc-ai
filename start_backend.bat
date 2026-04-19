@echo off
REM Agentic Doc AI v2 Backend Startup

echo Starting Agentic Doc AI v2 Backend...

REM Activate virtual environment
call venv_v2\Scripts\activate

REM Start backend
cd backend
python main_v2.py

echo Backend started on http://localhost:8000
pause
