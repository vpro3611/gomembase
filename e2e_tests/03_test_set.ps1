Import-Module "$PSScriptRoot\Test-Framework.psm1" -Force

$conn = Connect-DB
if (-not $conn) { exit 1 }

Write-Host "--- Testing Set Store ---" -ForegroundColor Magenta

# 1. Happy Path
$resp = Invoke-DB $conn "SADD" @("set1", "a", "b", "c")
Assert-Equal $true $resp.ok "Happy Path: SADD set1 a b c"
Assert-Equal 3 $resp.data[0] "Happy Path: SADD returns 3 added"

$resp = Invoke-DB $conn "SMEMBERS" @("set1")
Assert-Equal $true $resp.ok "Happy Path: SMEMBERS set1"
Assert-Equal 3 $resp.data.Count "Happy Path: SMEMBERS returns 3 items"

$resp = Invoke-DB $conn "SREM" @("set1", "b")
Assert-Equal $true $resp.ok "Happy Path: SREM set1 b"
Assert-Equal 1 $resp.data[0] "Happy Path: SREM returns 1 removed"

# 2. Error Path
$resp = Invoke-DB $conn "SADD" @("set2")
Assert-Equal $false $resp.ok "Error Path: SADD missing elements fails"

$resp = Invoke-DB $conn "SREM" @("set1", "x", "y")
Assert-Equal $true $resp.ok "Error Path: SREM non-existent returns 0"
Assert-Equal 0 $resp.data[0] "Error Path: SREM non-existent returns 0 removed"

# 3. Edge Cases
# Add duplicate elements
$resp = Invoke-DB $conn "SADD" @("set3", "dup", "dup")
Assert-Equal 1 $resp.data[0] "Edge Case: SADD dup dup returns 1 added"

# 4. Under Load
Write-Host "Running Set load test (1000 adds)..." -ForegroundColor DarkCyan
$sw = [System.Diagnostics.Stopwatch]::StartNew()
$loadFailures = 0
for ($i = 0; $i -lt 1000; $i++) {
    $r = Invoke-DB $conn "SADD" @("load_set", "item_$i")
    if (-not $r.ok) { $loadFailures++ }
}
$sw.Stop()
Assert-Equal 0 $loadFailures "Under Load: 1000 SADD operations"
Write-Host "Completed in $($sw.ElapsedMilliseconds) ms" -ForegroundColor DarkCyan

Write-TestSummary "Set Store"
Disconnect-DB $conn
