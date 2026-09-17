#Requires -Version 7.2
<#
.SYNOPSIS
Stage only public transport-probe image inputs into a new or empty directory.
.DESCRIPTION
No build, push, install, Azure command, journal, .env or src tree is included.
Use the resulting directory as the ACR/Docker build context, not the repository.
The deployment owner builds once, verifies the image digest, then supplies that
digest and approved nonsecret endpoint metadata to transport-probe-job.bicep.
#>
[CmdletBinding()]
param([Parameter(Mandatory)][string]$OutputDirectory)
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$root = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
$output = [IO.Path]::GetFullPath($OutputDirectory)
if ((Test-Path -LiteralPath $output) -and (
    -not (Test-Path -LiteralPath $output -PathType Container) -or
    @(Get-ChildItem -LiteralPath $output -Force).Count -ne 0
)) {
    throw 'OutputDirectory must be new or empty; existing files are never overwritten.'
}
$files = @(
    'Dockerfile.monitoring-transport-probe',
    'requirements.monitoring-transport-probe.txt',
    'scripts\hybrid_platform_probe.py',
    'scripts\monitoring_transport_probe.py',
    'scripts\monitoring_readiness_probe.py'
)
foreach ($name in $files) {
    if (-not (Test-Path -LiteralPath (Join-Path $root $name) -PathType Leaf)) {
        throw "Public probe build input is missing: $name"
    }
}
$null = New-Item -ItemType Directory -Path (Join-Path $output 'scripts') -Force
$manifest = @()
foreach ($name in $files) {
    $source = Join-Path $root $name
    $destination = Join-Path $output $name
    Copy-Item -LiteralPath $source -Destination $destination
    $manifest += @{
        path = $name
        sha256 = (Get-FileHash -LiteralPath $destination -Algorithm SHA256).Hash.ToLowerInvariant()
    }
}
@{ artifact = 'public-monitoring-transport-probe'; files = $manifest; imageBuilt = $false } |
    ConvertTo-Json -Depth 5 |
    Set-Content -LiteralPath (Join-Path $output 'transport-probe-package.json') -Encoding utf8
Write-Output "Public-only transport-probe build context: $output"
