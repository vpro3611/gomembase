Import-Module "$PSScriptRoot\Test-Framework.psm1" -Force

$conn = Connect-DB
if (-not $conn) { exit 1 }

Write-Host "--- Testing KV Store ---" -ForegroundColor Magenta

# 1. Happy Path
$resp = Invoke-DB $conn "SET" @("k1", "v1")
Assert-Equal $true $resp.ok "Happy Path: SET k1 v1"

$resp = Invoke-DB $conn "GET" @("k1")
Assert-Equal $true $resp.ok "Happy Path: GET k1 ok"
Assert-Equal "v1" $resp.data[0] "Happy Path: GET k1 data is v1"

$resp = Invoke-DB $conn "DEL" @("k1")
Assert-Equal $true $resp.ok "Happy Path: DEL k1 ok"

$resp = Invoke-DB $conn "GET" @("k1")
Assert-Equal $false $resp.ok "Happy Path: GET deleted key fails"

# 2. Error Path
$resp = Invoke-DB $conn "SET" @("k2")
Assert-Equal $false $resp.ok "Error Path: SET missing argument fails"

$resp = Invoke-DB $conn "GET" @()
Assert-Equal $false $resp.ok "Error Path: GET missing argument fails"

# 3. Edge Cases
# Empty string key and value
$resp = Invoke-DB $conn "SET" @("", "")
Assert-Equal $true $resp.ok "Edge Case: SET empty key and empty value"

$resp = Invoke-DB $conn "GET" @("")
Assert-Equal "" $resp.data[0] "Edge Case: GET empty key returns empty value"

# Special characters
$resp = Invoke-DB $conn "SET" @("!@#$%", "^&*()")
Assert-Equal $true $resp.ok "Edge Case: SET special characters"

$resp = Invoke-DB $conn "GET" @("!@#$%")
Assert-Equal "^&*()" $resp.data[0] "Edge Case: GET special characters"

# 4. Under Load
Write-Host "Running KV load test (1000 sets)..." -ForegroundColor DarkCyan
$sw = [System.Diagnostics.Stopwatch]::StartNew()
$loadFailures = 0
for ($i = 0; $i -lt 1000; $i++) {
    $r = Invoke-DB $conn "SET" @("load_k_$i", "load_v_$i")
    if (-not $r.ok) { $loadFailures++ }
}
$sw.Stop()
Assert-Equal 0 $loadFailures "Under Load: 1000 SET operations"
Write-Host "Completed in $($sw.ElapsedMilliseconds) ms" -ForegroundColor DarkCyan

Write-TestSummary "KV Store"
Disconnect-DB $conn
