$ErrorActionPreference = 'Stop'
. "$PSScriptRoot/common.ps1"
Push-Location $LabRoot
$LabPreviousJavaOptions = $env:JAVA_TOOL_OPTIONS
try {
    $env:JAVA_TOOL_OPTIONS = "$LabPreviousJavaOptions -Djdk.net.unixdomain.tmpdir=$LabRoot/target/nonexistent-socket-dir"
    & (Get-LabMaven) test
    if ($LASTEXITCODE -ne 0) { throw 'Tests failed. Inspect target/demo-evidence and target/surefire-reports.' }
} finally {
    $env:JAVA_TOOL_OPTIONS = $LabPreviousJavaOptions
    Pop-Location
}
