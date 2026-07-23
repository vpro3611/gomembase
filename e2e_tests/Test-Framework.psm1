function Connect-DB {
    param(
        [string]$IP = "127.0.0.1",
        [int]$Port = 6380,
        [switch]$NoCreate
    )
    $tcp = New-Object System.Net.Sockets.TcpClient
    try {
        $tcp.Connect($IP, $Port)
    } catch {
        Write-Error "Could not connect to $IP`:$Port"
        return $null
    }
    
    $stream = $tcp.GetStream()
    $reader = New-Object System.IO.StreamReader($stream)
    $writer = New-Object System.IO.StreamWriter($stream)
    $writer.AutoFlush = $true
    
    $connObj = @{
        Tcp = $tcp
        Stream = $stream
        Reader = $reader
        Writer = $writer
        UUIDs = @{}
    }
    
    # Create default instances for this connection to use
    if (-not $NoCreate) {
        foreach ($type in @("kv", "list", "set", "zset")) {
            $req = @{ method = "CREATE"; ds = $type }
            $json = $req | ConvertTo-Json -Compress
            $writer.WriteLine($json)
            $respStr = $reader.ReadLine()
            $resp = $respStr | ConvertFrom-Json
            if ($resp.ok) {
                $connObj.UUIDs[$type] = $resp.uuid
            }
        }
    }
    
    return $connObj
}

function Disconnect-DB {
    param($Conn)
    if ($Conn) {
        if ($Conn.Tcp) { $Conn.Tcp.Close() }
    }
}

function Invoke-DB {
    param(
        $Conn,
        [string]$Method,
        [array]$CmdArgs = @(),
        [string]$InstanceUUID = ""
    )
    
    $DS = "kv"
    if ($Method -match "^[LR](PUSH|POP)") { $DS = "list" }
    if ($Method -match "^S(ADD|MEMBERS|REM)") { $DS = "set" }
    if ($Method -match "^Z(ADD|RANGE)") { $DS = "zset" }
    
    $req = @{
        ds = $DS
        uuid = if ($InstanceUUID) { $InstanceUUID } else { $Conn.UUIDs[$DS] }
        method = $Method
        args = $CmdArgs
    }
    
    # We must properly format args to ensure they are valid JSON arrays.
    # ConvertTo-Json converts simple strings to "string".
    $json = $req | ConvertTo-Json -Depth 10 -Compress
    
    try {
        $Conn.Writer.WriteLine($json)
        $respStr = $Conn.Reader.ReadLine()
        if ([string]::IsNullOrEmpty($respStr)) {
            return $null
        }
        return $respStr | ConvertFrom-Json
    } catch {
        Write-Error "Failed to invoke DB: $_"
        return $null
    }
}

$global:TestFailures = 0
$global:TestPasses = 0

function Assert-True {
    param($Condition, [string]$Message)
    if ($Condition) {
        Write-Host "[PASS] $Message" -ForegroundColor Green
        $global:TestPasses++
    } else {
        Write-Host "[FAIL] $Message" -ForegroundColor Red
        $global:TestFailures++
    }
}

function Assert-Equal {
    param($Expected, $Actual, [string]$Message)
    
    # PowerShell comparison might be tricky with PSObjects. Convert both to JSON for strict equality if complex
    if ($Expected -is [array] -or $Expected -is [System.Management.Automation.PSCustomObject]) {
        $Expected = $Expected | ConvertTo-Json -Compress
    }
    if ($Actual -is [array] -or $Actual -is [System.Management.Automation.PSCustomObject]) {
        $Actual = $Actual | ConvertTo-Json -Compress
    }
    
    if ("$Expected" -eq "$Actual") {
        Write-Host "[PASS] $Message" -ForegroundColor Green
        $global:TestPasses++
    } else {
        Write-Host "[FAIL] $Message" -ForegroundColor Red
        Write-Host "       Expected: $Expected" -ForegroundColor DarkRed
        Write-Host "       Actual:   $Actual" -ForegroundColor DarkRed
        
        # If it's a response object with an error, print it!
        # PowerShell has trouble checking properties on boolean, so we can't just check $Actual.error.
        # But we can check if the parent script has $resp.error
        
        $global:TestFailures++
    }
}

function Write-TestSummary {
    param([string]$ModuleName)
    Write-Host "`n=== SUMMARY FOR $ModuleName ===" -ForegroundColor Cyan
    Write-Host "Passed: $global:TestPasses" -ForegroundColor Green
    if ($global:TestFailures -gt 0) {
        Write-Host "Failed: $global:TestFailures" -ForegroundColor Red
    } else {
        Write-Host "Failed: 0" -ForegroundColor Green
    }
    Write-Host "==========================`n" -ForegroundColor Cyan
    
    # Reset counters for the next suite if needed
    $global:TestPasses = 0
    $global:TestFailures = 0
}
