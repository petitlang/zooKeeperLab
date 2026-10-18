. (Join-Path $PSScriptRoot 'common.ps1')
Invoke-ProjectMaven @('package', '-DskipTests', 'dependency:build-classpath', '-Dmdep.includeScope=runtime', '-Dmdep.outputFile=target/runtime-classpath.txt')
