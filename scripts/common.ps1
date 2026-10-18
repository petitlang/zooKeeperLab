$LabRoot = Split-Path -Parent $PSScriptRoot

function Get-LabMaven {
    $LabMavenCommand = Get-Command mvn.cmd -ErrorAction SilentlyContinue
    if ($LabMavenCommand) { return $LabMavenCommand.Source }
    $LabBundledMaven = Get-ChildItem -Path "$env:ProgramFiles/JetBrains/IntelliJ IDEA */plugins/maven/lib/maven3/bin/mvn.cmd" -ErrorAction SilentlyContinue | Select-Object -First 1
    if ($LabBundledMaven) { return $LabBundledMaven.FullName }
    throw 'Maven not found. Install Maven or use IntelliJ IDEA with its bundled Maven.'
}

function Get-LabJava {
    if ($env:JAVA_HOME -and (Test-Path -LiteralPath "$env:JAVA_HOME/bin/java.exe")) {
        return "$env:JAVA_HOME/bin/java.exe"
    }
    return (Get-Command java.exe -ErrorAction Stop).Source
}

function Get-LabClasspath {
    $LabClasspathFile = Join-Path $LabRoot 'target/classpath.txt'
    if (!(Test-Path -LiteralPath $LabClasspathFile)) {
        throw 'Run ./scripts/build.ps1 first.'
    }
    return "$LabRoot/target/classes;" + (Get-Content -LiteralPath $LabClasspathFile -Raw).Trim()
}
