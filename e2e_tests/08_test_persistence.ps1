Import-Module "$PSScriptRoot\Test-Framework.psm1" -Force

Write-Host "--- Testing Persistence (WAL & Snapshot) ---" -ForegroundColor Magenta

$dbRoot = Split-Path $PSScriptRoot -Parent
$walPath = Join-Path $dbRoot "test.wal"
$rdbPath = Join-Path $dbRoot "test.rdb"
$serverExe = Join-Path $dbRoot "server.exe"

# Function to start the server in the background
function Start-Server {
    # Remove old wal and snap if they exist for a clean test
    if (Test-Path $walPath) { Remove-Item $walPath -Force }
    if (Test-Path $rdbPath) { Remove-Item $rdbPath -Force }
    
    $proc = Start-Process -PassThru -NoNewWindow -FilePath $serverExe -WorkingDirectory $dbRoot
    Start-Sleep -Seconds 1 # Wait for startup
    return $proc
}

function Start-Server-NoClean {
    $proc = Start-Process -PassThru -NoNewWindow -FilePath $serverExe -WorkingDirectory $dbRoot
    Start-Sleep -Seconds 1 # Wait for startup
    return $proc
}

# 1. Clean start
$serverProc = Start-Server

$conn = Connect-DB
if (-not $conn) {
    Write-Error "Failed to connect to server"
    Stop-Process -Id $serverProc.Id -Force
    exit 1
}

# 2. Write data (Happy Path under load)
Write-Host "Writing data to persist..." -ForegroundColor DarkCyan
$r = Invoke-DB $conn "SET" @("persist_k1", "persist_v1")
$r = Invoke-DB $conn "LPUSH" @("persist_l1", "persist_lv1")
$r = Invoke-DB $conn "SADD" @("persist_s1", "persist_sv1")
$r = Invoke-DB $conn "ZADD" @("persist_z1", "100", "persist_zv1")

# Wait 2 seconds for WAL flush (main.go flushes every 1 second)
Start-Sleep -Seconds 2

# 3. Kill the server
Write-Host "Killing server abruptly..." -ForegroundColor DarkCyan
$savedUUIDs = $conn.UUIDs
Disconnect-DB $conn
Stop-Process -Id $serverProc.Id -Force
Start-Sleep -Seconds 1

# 4. Restart server and verify
Write-Host "Restarting server to test recovery..." -ForegroundColor DarkCyan
$serverProc = Start-Server-NoClean
$conn = Connect-DB -NoCreate
$conn.UUIDs = $savedUUIDs

$r = Invoke-DB $conn "GET" @("persist_k1")
Assert-Equal "persist_v1" $r.data[0] "Recovery: GET persist_k1"

$r = Invoke-DB $conn "LPOP" @("persist_l1")
Assert-Equal "persist_lv1" $r.data[0] "Recovery: LPOP persist_l1"

$r = Invoke-DB $conn "SMEMBERS" @("persist_s1")
Assert-Equal "persist_sv1" $r.data[0] "Recovery: SMEMBERS persist_s1"

$r = Invoke-DB $conn "ZRANGE" @("persist_z1", "0", "1")
Assert-Equal "persist_zv1" $r.data[0] "Recovery: ZRANGE persist_z1"

Write-TestSummary "Persistence"
Disconnect-DB $conn
Stop-Process -Id $serverProc.Id -Force

# Clean up
if (Test-Path $walPath) { Remove-Item $walPath -Force }
if (Test-Path $rdbPath) { Remove-Item $rdbPath -Force }
