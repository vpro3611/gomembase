Import-Module "$PSScriptRoot\Test-Framework.psm1" -Force

$conn = Connect-DB
if (-not $conn) { exit 1 }

Write-Host "--- Testing List Store ---" -ForegroundColor Magenta

# 1. Happy Path
$resp = Invoke-DB $conn "LPUSH" @("list1", "a", "b")
if (-not $resp.ok) { Write-Host "LPUSH Error: $($resp.error)" -ForegroundColor Red }
Assert-Equal $true $resp.ok "Happy Path: LPUSH list1 a b"

$resp = Invoke-DB $conn "RPUSH" @("list1", "c")
Assert-Equal $true $resp.ok "Happy Path: RPUSH list1 c"

$resp = Invoke-DB $conn "LPOP" @("list1")
Assert-Equal "b" $resp.data[0] "Happy Path: LPOP returns 'b' (since we pushed a, then b on left -> b, a, c)"

$resp = Invoke-DB $conn "RPOP" @("list1")
Assert-Equal "c" $resp.data[0] "Happy Path: RPOP returns 'c'"

# 2. Error Path
$resp = Invoke-DB $conn "LPUSH" @("list_err")
Assert-Equal $false $resp.ok "Error Path: LPUSH missing values fails"

$resp = Invoke-DB $conn "LPOP" @("non_existent_list")
Assert-Equal $false $resp.ok "Error Path: LPOP non_existent_list fails"

# 3. Edge Cases
# Pop from empty list
$resp = Invoke-DB $conn "LPOP" @("list1") # pops 'a'
$resp = Invoke-DB $conn "LPOP" @("list1") # should fail, empty
Assert-Equal $false $resp.ok "Edge Case: LPOP empty list fails"

# Push huge payload
$hugeStr = "X" * 10000
$resp = Invoke-DB $conn "LPUSH" @("huge_list", $hugeStr)
Assert-Equal $true $resp.ok "Edge Case: LPUSH 10KB string"

$resp = Invoke-DB $conn "LPOP" @("huge_list")
Assert-Equal $hugeStr $resp.data[0] "Edge Case: LPOP 10KB string matches"

# 4. Under Load
Write-Host "Running List load test (1000 pushes)..." -ForegroundColor DarkCyan
$sw = [System.Diagnostics.Stopwatch]::StartNew()
$loadFailures = 0
for ($i = 0; $i -lt 1000; $i++) {
    $r = Invoke-DB $conn "RPUSH" @("load_list", "item_$i")
    if (-not $r.ok) { $loadFailures++ }
}
$sw.Stop()
Assert-Equal 0 $loadFailures "Under Load: 1000 RPUSH operations"
Write-Host "Completed in $($sw.ElapsedMilliseconds) ms" -ForegroundColor DarkCyan

Write-TestSummary "List Store"
Disconnect-DB $conn
