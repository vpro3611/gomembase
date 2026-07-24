Import-Module "$PSScriptRoot\Test-Framework.psm1" -Force

$conn = Connect-DB
if (-not $conn) { exit 1 }

Write-Host "--- Testing Mixed Instances & Transactions ---" -ForegroundColor Magenta

# 1. Happy Path
$resp = Invoke-DB $conn "CREATE" @()
Assert-Equal $true $resp.ok "Happy Path: Create Instance 1"
$inst1 = $resp.uuid

$resp = Invoke-DB $conn "CREATE" @()
Assert-Equal $true $resp.ok "Happy Path: Create Instance 2"
$inst2 = $resp.uuid

# Set on inst1
$resp = Invoke-DB $conn "SET" @("mix_k1", "inst1_v") $inst1
Assert-Equal $true $resp.ok "Happy Path: SET on inst1"

# Set on inst2
$resp = Invoke-DB $conn "SET" @("mix_k1", "inst2_v") $inst2
Assert-Equal $true $resp.ok "Happy Path: SET on inst2"

# Read back
$resp = Invoke-DB $conn "GET" @("mix_k1") $inst1
Assert-Equal "inst1_v" $resp.data[0] "Happy Path: GET on inst1"

$resp = Invoke-DB $conn "GET" @("mix_k1") $inst2
Assert-Equal "inst2_v" $resp.data[0] "Happy Path: GET on inst2"

# 2. Mixed Transactions
$resp = Invoke-DB $conn "MULTI"
$resp = Invoke-DB $conn "SET" @("mix_tx_k", "val1") $inst1
$resp = Invoke-DB $conn "SET" @("mix_tx_k", "val2") $inst2
$resp = Invoke-DB $conn "EXEC"

Assert-Equal $true $resp.ok "Mixed TX: EXEC ok"
Assert-Equal 2 $resp.data.Count "Mixed TX: EXEC returns 2 results"

$resp = Invoke-DB $conn "GET" @("mix_tx_k") $inst1
Assert-Equal "val1" $resp.data[0] "Mixed TX: GET inst1 val1"

$resp = Invoke-DB $conn "GET" @("mix_tx_k") $inst2
Assert-Equal "val2" $resp.data[0] "Mixed TX: GET inst2 val2"

Write-TestSummary "Mixed Instances & Transactions"
Disconnect-DB $conn
