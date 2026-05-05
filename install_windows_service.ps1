# install_windows_service.ps1 — Run as Administrator
# powershell -ExecutionPolicy Bypass -File install_windows_service.ps1

$ErrorActionPreference = "Stop"

$ServiceName = "BobisQuoteCandleService"
$EngineDir   = Split-Path -Parent $MyInvocation.MyCommand.Path
$AppExe      = Join-Path $EngineDir ".venv\Scripts\market-data.exe"
$NssmExe     = Join-Path $EngineDir "nssm\nssm-2.24\win64\nssm.exe"
$LogDir      = Join-Path $EngineDir "logs"

# ── Pre-flight ────────────────────────────────────────────────────────────────
if (-not (Test-Path $AppExe)) {
    Write-Error "Executable not found: $AppExe"
    exit 1
}

if (-not (Test-Path $NssmExe)) {
    Write-Host "Downloading NSSM..."
    $ZipPath = Join-Path $EngineDir "nssm.zip"
    Invoke-WebRequest -Uri "https://nssm.cc/release/nssm-2.24.zip" -OutFile $ZipPath
    Expand-Archive $ZipPath -DestinationPath (Join-Path $EngineDir "nssm") -Force
    Remove-Item $ZipPath
    $NssmExe = Join-Path $EngineDir "nssm\nssm-2.24\win64\nssm.exe"
}

New-Item -ItemType Directory -Force -Path $LogDir | Out-Null

# ── Remove existing cleanly ───────────────────────────────────────────────────
$exists = sc.exe query $ServiceName 2>$null
if ($LASTEXITCODE -eq 0) {
    Write-Host "Stopping and removing existing service..."
    & $NssmExe stop   $ServiceName confirm 2>$null | Out-Null
    Start-Sleep -Seconds 2
    & $NssmExe remove $ServiceName confirm 2>$null | Out-Null
    Start-Sleep -Seconds 2
}

# Kill any orphan processes from previous runs
Get-Process | Where-Object {
    $_.Path -like "*mt5-trading-api*" -or $_.ProcessName -like "market-data*"
} | Stop-Process -Force -ErrorAction SilentlyContinue

# ── Install ───────────────────────────────────────────────────────────────────
Write-Host "Installing service..."

& $NssmExe install $ServiceName $AppExe

# Working directory
& $NssmExe set $ServiceName AppDirectory $EngineDir

# Logging
& $NssmExe set $ServiceName AppStdout "$LogDir\stdout.log"
& $NssmExe set $ServiceName AppStderr "$LogDir\stderr.log"
& $NssmExe set $ServiceName AppRotateFiles 1
& $NssmExe set $ServiceName AppRotateBytes 10485760

# Graceful shutdown (CRITICAL)
& $NssmExe set $ServiceName AppStopMethodConsole 15000
& $NssmExe set $ServiceName AppStopMethodWindow  15000
& $NssmExe set $ServiceName AppStopMethodThreads 15000

# Restart behavior (controlled)
& $NssmExe set $ServiceName AppThrottle 5000        # prevent rapid restart loops
& $NssmExe set $ServiceName AppExit Default Restart

# Service metadata
& $NssmExe set $ServiceName Start SERVICE_AUTO_START
& $NssmExe set $ServiceName DisplayName "BobisQuoteCandleService (MT5)"
& $NssmExe set $ServiceName Description "MT5 FastAPI candle data service"

# Windows-level recovery (bounded)
sc.exe failure $ServiceName reset= 300 actions= restart/5000/restart/15000/""/0 | Out-Null

# ── Start ─────────────────────────────────────────────────────────────────────
Write-Host "Starting service..."
& $NssmExe start $ServiceName

Start-Sleep -Seconds 3
& $NssmExe status $ServiceName

Write-Host ""
Write-Host "Logs:"
Write-Host "  Get-Content '$LogDir\stderr.log' -Tail 50 -Wait"