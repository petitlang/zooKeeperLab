param([Parameter(Mandatory)][ValidateRange(1,5)][int]$Id, [int]$BasePort = 5000, [string]$DataDir = 'data')
. (Join-Path $PSScriptRoot 'common.ps1')
$classpath = Get-RuntimeClasspath
Push-Location $script:ProjectRoot
try {
    & java (Get-SelectorOption) -cp $classpath fr.isep.node.NodeServer --id $Id --base-port $BasePort --data-dir $DataDir
    if ($LASTEXITCODE -ne 0) { throw "Node exited with code $LASTEXITCODE" }
} finally { Pop-Location }
