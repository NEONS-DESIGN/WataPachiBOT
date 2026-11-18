@echo off
setlocal
rem Toggle the network priority settings for WataPachiBOT (tools\net_priority.py).
rem No argument shows a menu. Arguments: on / off / status / check
rem Keep this file ASCII only: cmd parses batch files in the OEM code page, so UTF-8 text can break lines.

rem Prefer PowerShell 7 (pwsh; the Store version is an app execution alias in the user's WindowsApps), else Windows PowerShell 5.1.
set "PS_EXE=powershell"
where pwsh >nul 2>&1 && set "PS_EXE=pwsh"

set "ELEVATED="
if /i "%~1"=="/elevated" (
	set "ELEVATED=1"
	shift /1
)

rem fltmc succeeds only as administrator and, unlike "net session", does not depend on the Server service.
fltmc >nul 2>&1
if not errorlevel 1 goto :run
if defined ELEVATED (
	echo [ERROR] Administrator rights could not be obtained.
	pause
	exit /b 1
)
if "%~1"=="" (
	%PS_EXE% -NoProfile -Command "Start-Process -FilePath '%~f0' -ArgumentList '/elevated' -Verb RunAs"
) else (
	%PS_EXE% -NoProfile -Command "Start-Process -FilePath '%~f0' -ArgumentList '/elevated %~1' -Verb RunAs"
)
if errorlevel 1 (
	echo [ERROR] Elevation was cancelled or failed.
	pause
	exit /b 1
)
exit /b 0

:run
cd /d "%~dp0.."
rem The script needs only the standard library, so any Python 3.11+ works.
rem Order: the worker environment in worker\envs\current.json, then worker\.venv, then the py launcher.
set "PY_EXE="
for /f "usebackq delims=" %%P in (`%PS_EXE% -NoProfile -Command "try { $r = Get-Content -LiteralPath 'worker\envs\current.json' -Raw -ErrorAction Stop | ConvertFrom-Json; $p = ([string]$r.python) -replace '/', '\'; if ($p -and (Test-Path -LiteralPath $p -PathType Leaf)) { $p } } catch {}"`) do set "PY_EXE=%%P"
if not defined PY_EXE if exist "worker\.venv\Scripts\python.exe" set "PY_EXE=worker\.venv\Scripts\python.exe"
if not defined PY_EXE set "PY_EXE=py"
"%PY_EXE%" "%~dp0net_priority.py" %1
set "RC=%ERRORLEVEL%"
if "%RC%"=="9009" echo [ERROR] Python was not found. Create the worker environment with "bin\watapachi-bot.exe update-worker" first.

rem The elevated window closes on exit, so pause to let the result be read. Always pause on failure.
if not "%RC%"=="0" (
	pause
) else if defined ELEVATED if not "%~1"=="" pause
exit /b %RC%
