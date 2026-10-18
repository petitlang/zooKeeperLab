$ErrorActionPreference = 'Stop'
. "$PSScriptRoot/common.ps1"
Push-Location $LabRoot
try {
    & (Get-LabMaven) compile dependency:build-classpath '-Dmdep.outputFile=target/classpath.txt' -q
    if ($LASTEXITCODE -ne 0) { throw 'Maven build failed.' }
    Write-Host 'Build complete. Run ./scripts/run-node.ps1 -Id 1'
} finally {
    Pop-Location
}
