Import-Module "$PSScriptRoot\Test-Framework.psm1" -Force

$subConn = Connect-DB
$pubConn = Connect-DB

if (-not $subConn -or -not $pubConn) { exit 1 }

Write-Host "--- Testing Pub/Sub System ---" -ForegroundColor Magenta

# 1. Happy Path
$resp = Invoke-DB $subConn "SUBSCRIBE" @("channel1")
Assert-Equal "subscribe" $resp.data[0] "Happy Path: Subscribe to channel1"

# We must send PUBLISH, then read the push message on the subscriber stream
$pubReq = @{ method = "PUBLISH"; args = @("channel1", "hello!") }
$pubConn.Writer.WriteLine(($pubReq | ConvertTo-Json -Compress))
$pubRespStr = $pubConn.Reader.ReadLine()
$pubResp = $pubRespStr | ConvertFrom-Json
Assert-Equal 1 $pubResp.data[0] "Happy Path: PUBLISH returns 1 recipient"

# Read pushed message
$pushStr = $subConn.Reader.ReadLine()
$pushMsg = $pushStr | ConvertFrom-Json
Assert-Equal "message" $pushMsg.type "Happy Path: push type is message"
Assert-Equal "channel1" $pushMsg.channel "Happy Path: push channel matches"
Assert-Equal "hello!" $pushMsg.data "Happy Path: push data matches"

# 2. Error Path
$resp = Invoke-DB $subConn "SET" @("k", "v")
Assert-Equal $false $resp.ok "Error Path: standard commands blocked in subscriber mode"

# 3. Edge Cases
# Publish to no one
$pubReq = @{ method = "PUBLISH"; args = @("nobody", "hi") }
$pubConn.Writer.WriteLine(($pubReq | ConvertTo-Json -Compress))
$pubRespStr = $pubConn.Reader.ReadLine()
$pubResp = $pubRespStr | ConvertFrom-Json
Assert-Equal 0 $pubResp.data[0] "Edge Case: PUBLISH to 0 recipients returns 0"

# 4. Under Load (Many publishes rapidly)
Write-Host "Running PubSub load test (1000 messages)..." -ForegroundColor DarkCyan
$sw = [System.Diagnostics.Stopwatch]::StartNew()
for ($i = 0; $i -lt 1000; $i++) {
    $pubReq = @{ method = "PUBLISH"; args = @("channel1", "load_$i") }
    $pubConn.Writer.WriteLine(($pubReq | ConvertTo-Json -Compress))
    $null = $pubConn.Reader.ReadLine() # consume response
}
$sw.Stop()

# Now we need to read 1000 messages on the subscriber side
$loadFails = 0
for ($i = 0; $i -lt 1000; $i++) {
    if ($subConn.Tcp.Available) {
        $msgStr = $subConn.Reader.ReadLine()
    } else {
        # Wait briefly for delivery
        Start-Sleep -Milliseconds 1
        if ($subConn.Tcp.Available) {
            $msgStr = $subConn.Reader.ReadLine()
        } else {
            $loadFails++
            continue
        }
    }
}
Assert-Equal 0 $loadFails "Under Load: 1000 messages received without loss"
Write-Host "Completed in $($sw.ElapsedMilliseconds) ms" -ForegroundColor DarkCyan

Write-TestSummary "Pub/Sub System"
Disconnect-DB $subConn
Disconnect-DB $pubConn
