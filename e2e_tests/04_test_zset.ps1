Import-Module "$PSScriptRoot\Test-Framework.psm1" -Force

$conn = Connect-DB
if (-not $conn) { exit 1 }

Write-Host "--- Testing ZSet Store ---" -ForegroundColor Magenta

# 1. Happy Path
$resp = Invoke-DB $conn "ZADD" @("zset1", "10", "player1", "20", "player2")
Assert-Equal $true $resp.ok "Happy Path: ZADD zset1"
Assert-Equal 2 $resp.data[0] "Happy Path: ZADD returns 2 added"

$resp = Invoke-DB $conn "ZRANGE" @("zset1", "0", "1")
Assert-Equal $true $resp.ok "Happy Path: ZRANGE zset1 0 1"
Assert-Equal "player1" $resp.data[0] "Happy Path: First element is player1"
Assert-Equal "10" $resp.data[1] "Happy Path: First element score is 10"
Assert-Equal "player2" $resp.data[2] "Happy Path: Second element is player2"
Assert-Equal "20" $resp.data[3] "Happy Path: Second element score is 20"

# 2. Error Path
$resp = Invoke-DB $conn "ZADD" @("zset1", "invalid_score", "player3")
Assert-Equal $false $resp.ok "Error Path: ZADD with non-numeric score fails"

$resp = Invoke-DB $conn "ZRANGE" @("zset1", "a", "b")
Assert-Equal $false $resp.ok "Error Path: ZRANGE with invalid indices fails"

# 3. Edge Cases
# ZRANGE out of bounds
$resp = Invoke-DB $conn "ZRANGE" @("zset1", "0", "100")
Assert-Equal 4 $resp.data.Count "Edge Case: ZRANGE out of bounds caps at length (returns pairs)"

# 4. Under Load
Write-Host "Running ZSet load test (1000 adds)..." -ForegroundColor DarkCyan
$sw = [System.Diagnostics.Stopwatch]::StartNew()
$loadFailures = 0
for ($i = 0; $i -lt 1000; $i++) {
    $r = Invoke-DB $conn "ZADD" @("load_zset", "$i", "p_$i")
    if (-not $r.ok) { $loadFailures++ }
}
$sw.Stop()
Assert-Equal 0 $loadFailures "Under Load: 1000 ZADD operations"
Write-Host "Completed in $($sw.ElapsedMilliseconds) ms" -ForegroundColor DarkCyan

Write-TestSummary "ZSet Store"
Disconnect-DB $conn
