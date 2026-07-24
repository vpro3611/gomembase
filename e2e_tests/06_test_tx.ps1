Import-Module "$PSScriptRoot\Test-Framework.psm1" -Force

$conn = Connect-DB
if (-not $conn) { exit 1 }

Write-Host "--- Testing Transactions ---" -ForegroundColor Magenta

# 1. Happy Path
$resp = Invoke-DB $conn "MULTI"
Assert-Equal $true $resp.ok "Happy Path: MULTI ok"

$resp = Invoke-DB $conn "SET" @("tx_k1", "tx_v1")
Assert-Equal "QUEUED" $resp.data[0] "Happy Path: SET queued"

$resp = Invoke-DB $conn "LPUSH" @("tx_l1", "tx_lv1")
Assert-Equal "QUEUED" $resp.data[0] "Happy Path: LPUSH queued"

$resp = Invoke-DB $conn "EXEC"
Assert-Equal $true $resp.ok "Happy Path: EXEC ok"
Assert-Equal 2 $resp.data.Count "Happy Path: EXEC returns 2 responses"
Assert-Equal $true $resp.data[0].ok "Happy Path: SET succeeded in TX"
Assert-Equal $true $resp.data[1].ok "Happy Path: LPUSH succeeded in TX"

# 2. Error Path
$resp = Invoke-DB $conn "EXEC"
Assert-Equal $false $resp.ok "Error Path: EXEC without MULTI fails"

$resp = Invoke-DB $conn "MULTI"
$resp = Invoke-DB $conn "MULTI"
Assert-Equal $false $resp.ok "Error Path: Nested MULTI fails"
$resp = Invoke-DB $conn "DISCARD"

# 3. Edge Cases
# TX with errors inside (e.g. invalid command arguments)
$resp = Invoke-DB $conn "MULTI"
$resp = Invoke-DB $conn "SET" @("valid_key", "valid_val")
$resp = Invoke-DB $conn "ZADD" @("invalid", "not_a_number", "v")
$resp = Invoke-DB $conn "EXEC"
Assert-Equal $false $resp.ok "Edge Case: EXEC with invalid command fails entirely"

# Ensure rollback occurred
$resp = Invoke-DB $conn "GET" @("valid_key")
Assert-Equal $false $resp.ok "Edge Case: rollback successful (key not set)"

# 4. Under Load
Write-Host "Running Transaction load test (100 txs with 10 cmds each)..." -ForegroundColor DarkCyan
$sw = [System.Diagnostics.Stopwatch]::StartNew()
$loadFailures = 0
for ($i = 0; $i -lt 100; $i++) {
    $r = Invoke-DB $conn "MULTI"
    for ($j = 0; $j -lt 10; $j++) {
        $null = Invoke-DB $conn "SET" @("load_tx_k_$i_$j", "v")
    }
    $r = Invoke-DB $conn "EXEC"
    if (-not $r.ok) { $loadFailures++ }
}
$sw.Stop()
Assert-Equal 0 $loadFailures "Under Load: 100 EXEC operations"
Write-Host "Completed in $($sw.ElapsedMilliseconds) ms" -ForegroundColor DarkCyan

Write-TestSummary "Transactions"
Disconnect-DB $conn
