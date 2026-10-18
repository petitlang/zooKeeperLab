param(
    [ValidateRange(1,5)][int]$Node = 1,
    [string]$Client = 'A',
    [ValidateRange(0,65530)][int]$BasePort = 5000,
    [string]$Command = ''
)
$ErrorActionPreference = 'Stop'
. "$PSScriptRoot/common.ps1"
Push-Location $LabRoot
try {
    $LabClientArgs = @("-Djdk.net.unixdomain.tmpdir=$LabRoot/target/nonexistent-socket-dir", '-cp', (Get-LabClasspath), 'fr.isep.client.ClientApp', '--node', $Node, '--client', $Client, '--base-port', $BasePort)
    if ($Command) { $LabClientArgs += $Command.Trim() -split '\s+' }
    & (Get-LabJava) @LabClientArgs
    if ($LASTEXITCODE -ne 0) { throw 'Client stopped with an error.' }
} finally {
    Pop-Location
}
