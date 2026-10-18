$ErrorActionPreference = 'Stop'
$script:ProjectRoot = Split-Path $PSScriptRoot -Parent

function Find-Maven {
    $command = Get-Command mvn.cmd -ErrorAction SilentlyContinue
    if ($command) { return $command.Source }
    $jetbrains = Join-Path $env:ProgramFiles 'JetBrains'
    if (Test-Path -LiteralPath $jetbrains) {
        foreach ($installation in Get-ChildItem -LiteralPath $jetbrains -Directory) {
            $candidate = Join-Path $installation.FullName 'plugins\maven\lib\maven3\bin\mvn.cmd'
            if (Test-Path -LiteralPath $candidate) { return $candidate }
        }
    }
    throw 'Maven not found. Install Maven or use the Maven bundled with IntelliJ IDEA.'
}

function Get-SelectorOption {
    # A nonexistent directory avoids a Windows JDK 17 selector issue on this machine.
    $selectorPath = Join-Path $script:ProjectRoot 'target\nonexistent-selector-dir'
    if (Test-Path -LiteralPath $selectorPath) { throw "This path must stay nonexistent: $selectorPath" }
    return "-Djdk.net.unixdomain.tmpdir=$selectorPath"
}

function Invoke-ProjectMaven([string[]] $Arguments) {
    $previousOptions = $env:JAVA_TOOL_OPTIONS
    Push-Location $script:ProjectRoot
    try {
        $env:JAVA_TOOL_OPTIONS = "$previousOptions $(Get-SelectorOption)".Trim()
        & (Find-Maven) @Arguments
        if ($LASTEXITCODE -ne 0) { throw "Maven failed with exit code $LASTEXITCODE" }
    } finally {
        $env:JAVA_TOOL_OPTIONS = $previousOptions
        Pop-Location
    }
}

function Get-RuntimeClasspath {
    $classpathFile = Join-Path $script:ProjectRoot 'target\runtime-classpath.txt'
    if (!(Test-Path -LiteralPath $classpathFile)) { & (Join-Path $PSScriptRoot 'build.ps1') }
    return "$(Join-Path $script:ProjectRoot 'target\classes');$((Get-Content -LiteralPath $classpathFile -Raw).Trim())"
}
