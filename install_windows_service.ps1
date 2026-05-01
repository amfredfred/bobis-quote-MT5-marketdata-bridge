# install_windows_service.ps1 — Run as Administrator
# powershell -ExecutionPolicy Bypass -File install_windows_service.ps1

$ServiceName = "BobisQuoteCandleService"
$EngineDir   = Split-Path -Parent $MyInvocation.MyCommand.Path
$PythonExe   = Join-Path $EngineDir ".venv\Scripts\python.exe"
$MainScript = Join-Path $EngineDir ".venv\Scripts\market-data.exe"
$NssmExe     = Join-Path $EngineDir ".\nssm\nssm-2.24\win64\nssm.exe"
$LogDir      = Join-Path $EngineDir "logs"

# ── Pre-flight ────────────────────────────────────────────────────────────────
if (-not (Test-Path $PythonExe)) { Write-Error "Venv not found: $PythonExe"; exit 1 }
if (-not (Test-Path $NssmExe))   {
    Write-Host "Downloading NSSM..."
    Invoke-WebRequest -Uri "https://nssm.cc/release/nssm-2.24.zip" -OutFile "$EngineDir\nssm.zip"
    Expand-Archive "$EngineDir\nssm.zip" -DestinationPath "$EngineDir\nssm" -Force
    Remove-Item "$EngineDir\nssm.zip"
    $NssmExe = Join-Path $EngineDir "nssm\nssm-2.24\win64\nssm.exe"
}

# ── Remove existing ───────────────────────────────────────────────────────────
if ((& $NssmExe status $ServiceName 2>$null) -ne $null) {
    Write-Host "Removing existing service..."
    & $NssmExe stop   $ServiceName confirm 2>$null | Out-Null
    & $NssmExe remove $ServiceName confirm 2>$null | Out-Null
    Start-Sleep -Seconds 2
}

# ── Install ───────────────────────────────────────────────────────────────────
Write-Host "Installing $ServiceName..."
New-Item -ItemType Directory -Force -Path $LogDir | Out-Null

& $NssmExe install $ServiceName $PythonExe $MainScript
& $NssmExe set $ServiceName AppDirectory  $EngineDir
& $NssmExe set $ServiceName AppStdout     "$LogDir\service_stdout.log"
& $NssmExe set $ServiceName AppStderr     "$LogDir\service_stderr.log"
& $NssmExe set $ServiceName AppRotateFiles 1
& $NssmExe set $ServiceName AppRotateBytes 10485760
& $NssmExe set $ServiceName Start         SERVICE_AUTO_START
& $NssmExe set $ServiceName DisplayName   "BobisQuoteCandleService (MT5)"
& $NssmExe set $ServiceName Description   "FastAPI MT5 candle data server on port 8000."
& $NssmExe set $ServiceName AppThrottle 15000  # wait 15s before starting

# Auto-restart on failure: 5s, 10s, 30s
sc.exe failure $ServiceName reset= 60 actions= restart/5000/restart/10000/restart/30000 | Out-Null

# ── Start ─────────────────────────────────────────────────────────────────────
Write-Host "Starting service..."
& $NssmExe start $ServiceName
Start-Sleep -Seconds 5
& $NssmExe status $ServiceName

Write-Host ""
Write-Host "Logs: Get-Content '$LogDir\service_stderr.log' -Tail 50 -Wait"