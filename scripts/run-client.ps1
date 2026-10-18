param([ValidateRange(1,5)][int]$Node = 1, [string]$Client = 'A', [int]$BasePort = 5000, [string]$Command)
. (Join-Path $PSScriptRoot 'common.ps1')
$classpath = Get-RuntimeClasspath
$clientArguments = @('--node', "$Node", '--client', $Client, '--base-port', "$BasePort")
if ($Command) { $clientArguments += @('--command', $Command) }
Push-Location $script:ProjectRoot
try {
    & java (Get-SelectorOption) -cp $classpath fr.isep.client.ClientApp @clientArguments
    if ($LASTEXITCODE -ne 0) { throw "Client exited with code $LASTEXITCODE" }
} finally { Pop-Location }
