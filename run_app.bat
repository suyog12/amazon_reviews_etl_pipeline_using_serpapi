@echo off
title Amazon Reviews ETL Pipeline
echo ===============================
echo  Amazon Reviews ETL Pipeline
echo ===============================
echo.

REM Activate virtual environment (optional)
if exist venv (
    echo Activating virtual environment...
    call venv\Scripts\activate
)

REM Start Flask backend in a new terminal
echo Starting Flask API server...
start cmd /k "python api.py"

REM Wait for API to boot up
timeout /t 5 >nul

REM Launch the frontend in default browser
echo Opening dashboard...
start index.html

echo.
echo Application started successfully!
echo Backend: http://localhost:5000
echo.
pause
