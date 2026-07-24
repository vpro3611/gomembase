Write-Host "=========================================" -ForegroundColor Yellow
Write-Host "     GObase E2E Brutal Test Suite        " -ForegroundColor Yellow
Write-Host "=========================================" -ForegroundColor Yellow

# Clean up before we begin
if (Test-Path "..\test.wal") { Remove-Item "..\test.wal" -Force }
if (Test-Path "..\test.rdb") { Remove-Item "..\test.rdb" -Force }

Write-Host "`n[1/2] Building and starting server for active tests..." -ForegroundColor Green
Set-Location ".."
go build -o server.exe main.go
$serverProc = Start-Process -PassThru -NoNewWindow -FilePath ".\server.exe"
Start-Sleep -Seconds 1 # Give it time to start
Set-Location "e2e_tests"

try {
    .\01_test_kv.ps1
    .\02_test_list.ps1
    .\03_test_set.ps1
    .\04_test_zset.ps1
    .\05_test_pubsub.ps1
    .\06_test_tx.ps1
    .\07_test_mix.ps1
} finally {
    Write-Host "`n[2/2] Stopping server and running persistence tests..." -ForegroundColor Green
    Stop-Process -Id $serverProc.Id -Force -ErrorAction SilentlyContinue
    Start-Sleep -Seconds 1
}

# The persistence test manages its own server process
Set-Location ".."
.\e2e_tests\08_test_persistence.ps1

Write-Host "=========================================" -ForegroundColor Yellow
Write-Host "          ALL TESTS COMPLETED            " -ForegroundColor Yellow
Write-Host "=========================================" -ForegroundColor Yellow
