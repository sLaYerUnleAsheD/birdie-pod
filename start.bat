@echo off
echo ========================================================
echo  Starting Aegis Modernize Viewer (Challenge 03)
echo ========================================================

:: Check if Node.js is installed (fastest & native in user PATH)
where node >nul 2>nul
if %ERRORLEVEL% equ 0 (
    echo [OK] Found Node.js runtime. Starting server...
    start "" "http://localhost:8000/#/modernize"
    node serve.js
    goto :eof
)

:: Fallback to Python launcher 'py'
where py >nul 2>nul
if %ERRORLEVEL% equ 0 (
    echo [OK] Found Python launcher. Starting server...
    start "" "http://localhost:8000/#/modernize"
    py serve.py
    goto :eof
)

:: Fallback to python
where python >nul 2>nul
if %ERRORLEVEL% equ 0 (
    echo [OK] Found Python. Starting server...
    start "" "http://localhost:8000/#/modernize"
    python serve.py
    goto :eof
)

echo [!] Could not locate Node.js or Python runtime automatically.
pause

