param(
    [Parameter(Mandatory=$true)][ValidateRange(1,5)][int]$Id,
    [ValidateRange(0,65530)][int]$BasePort = 5000,
    [string]$DataDir = 'data'
)
$ErrorActionPreference = 'Stop'
. "$PSScriptRoot/common.ps1"
Push-Location $LabRoot
try {
    # A nonexistent directory makes Windows JDK 17 use TCP for its internal selector.
    & (Get-LabJava) "-Djdk.net.unixdomain.tmpdir=$LabRoot/target/nonexistent-socket-dir" -cp (Get-LabClasspath) fr.isep.node.NodeServer --id $Id --base-port $BasePort --data-dir $DataDir
    if ($LASTEXITCODE -ne 0) { throw 'Node stopped with an error.' }
} finally {
    Pop-Location
}
