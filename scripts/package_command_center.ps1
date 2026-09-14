#Requires -Version 7.2
<#
.SYNOPSIS
Build and package the command center for Python/Oryx ZIP deployment.
.DESCRIPTION
Requires Node/npm and Python >=3.11. Uses npm's typecheck script, or tsc --build when
there is no typecheck script, followed by npm run build. Use -RestoreDependencies
to explicitly restore the frontend lockfile first.

Only src, pyproject.toml, README.md, LICENSE, scenarios, mock and the built
command-center\dist are packaged. requirements.txt is generated inside the ZIP
as .[web,azure]. No root dependency manifest is changed. The ZIP must be outside
the repository, and an existing output is never overwritten without -Force.
The temporary staging directory is the only directory removed by this script.
.EXAMPLE
.\scripts\package_command_center.ps1 -OutputPath D:\artifacts\command-center.zip
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory)]
    [string]$OutputPath,
    [string]$Python,
    [switch]$RestoreDependencies,
    [switch]$Force
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$repo = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
$frontend = Join-Path $repo 'command-center'
$destination = [IO.Path]::GetFullPath($OutputPath)
$comparison = [StringComparison]::OrdinalIgnoreCase
if ($destination.StartsWith($repo + [IO.Path]::DirectorySeparatorChar, $comparison)) {
    throw 'Choose an output ZIP outside the repository so it cannot be committed.'
}
if ([IO.Path]::GetExtension($destination) -ine '.zip') {
    throw 'OutputPath must name a .zip file.'
}
if ((Test-Path -LiteralPath $destination) -and -not $Force) {
    throw "Output already exists: $destination. Choose a new name or use -Force."
}
if (-not $Python) {
    $localPython = Join-Path $repo '.venv\Scripts\python.exe'
    $Python = if (Test-Path -LiteralPath $localPython) { $localPython } else { 'python' }
}

function Invoke-Checked {
    param([string]$Executable, [string[]]$Arguments)
    & $Executable @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "$Executable failed with exit code $LASTEXITCODE. No deployment package was produced."
    }
}

function Test-ExcludedPath {
    param([string]$RelativePath)
    $normalized = $RelativePath.Replace('\', '/')
    return (
        $normalized -match '(^|/)(\.git|\.hg|\.svn|\.azure|\.venv|venv|runs|node_modules|__pycache__|\.pytest_cache|\.ruff_cache|\.mypy_cache|[^/]+\.egg-info)(/|$)' -or
        $normalized -match '(^|/)\.env[^/]*$' -or
        $normalized -match '(?i)(^|/)(\.npmrc|\.pypirc|local\.settings\.json|\.?credentials|credentials?[^/]*\.(json|ya?ml|ini|cfg|config|xml|txt)|secrets?[^/]*\.(json|ya?ml)|tokens?[^/]*\.json)$' -or
        $normalized -match '(?i)\.(pyc|pyo|pem|key|pfx|p12|publishsettings|pubxml|user|map|zip)$'
    )
}

function Copy-PackagePath {
    param([string]$Source, [string]$RelativePath, [string]$Payload)
    $item = Get-Item -LiteralPath $Source -Force
    if (Test-ExcludedPath $RelativePath) { return }
    if (($item.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0) {
        throw "Package input is a symbolic link or reparse point: $RelativePath"
    }
    $target = Join-Path $Payload $RelativePath
    if ($item.PSIsContainer) {
        $null = New-Item -ItemType Directory -Path $target -Force
        foreach ($child in Get-ChildItem -LiteralPath $Source -Force) {
            Copy-PackagePath $child.FullName (Join-Path $RelativePath $child.Name) $Payload
        }
    } else {
        $null = New-Item -ItemType Directory -Path ([IO.Path]::GetDirectoryName($target)) -Force
        Copy-Item -LiteralPath $Source -Destination $target
    }
}

$manifestPath = Join-Path $frontend 'package.json'
if (-not (Test-Path -LiteralPath $manifestPath -PathType Leaf)) {
    throw 'command-center\package.json is missing. Finish the frontend before packaging.'
}
$manifest = Get-Content -LiteralPath $manifestPath -Raw | ConvertFrom-Json -AsHashtable
if (-not $manifest.ContainsKey('scripts') -or -not $manifest.scripts.ContainsKey('build')) {
    throw 'The frontend must define an npm build script.'
}
Push-Location $frontend
try {
    if ($RestoreDependencies) {
        if (-not (Test-Path -LiteralPath 'package-lock.json' -PathType Leaf)) {
            throw 'Restoring dependencies requires command-center\package-lock.json.'
        }
        Invoke-Checked 'npm' @('ci', '--no-audit', '--no-fund')
    }
    if ($manifest.scripts.ContainsKey('typecheck')) {
        Invoke-Checked 'npm' @('run', 'typecheck')
    } else {
        Invoke-Checked 'npm' @('exec', '--no', '--', 'tsc', '--build')
    }
    Invoke-Checked 'npm' @('run', 'build')
} finally {
    Pop-Location
}

$stage = Join-Path ([IO.Path]::GetTempPath()) ('triage-command-center-' + [guid]::NewGuid().ToString('N'))
$null = New-Item -ItemType Directory -Path $stage
$payload = Join-Path $stage 'payload'
$zipPath = Join-Path $stage 'command-center.zip'
try {
    $null = New-Item -ItemType Directory -Path $payload
    foreach ($relative in @('src', 'pyproject.toml', 'README.md', 'LICENSE', 'scenarios', 'mock', 'command-center\dist')) {
        $source = Join-Path $repo $relative
        if (-not (Test-Path -LiteralPath $source)) {
            throw "Required package input is missing: $relative"
        }
        Copy-PackagePath $source $relative $payload
    }
    Set-Content -LiteralPath (Join-Path $payload 'requirements.txt') -Value '.[web,azure]' -Encoding utf8NoBOM

    # Reuse the repository's credential gate on the staging tree, not just git's
    # tracked files. Staging outside the repository activates its tree fallback.
    $scan = @'
import importlib.util
import sys
import tomllib
from pathlib import Path

root = Path(sys.argv[2])
manifest = tomllib.loads((root / "pyproject.toml").read_text(encoding="utf-8"))
extras = manifest.get("project", {}).get("optional-dependencies", {})
if not all(extras.get(name) for name in ("web", "azure")):
    raise SystemExit("The package requires nonempty web and azure dependency extras.")
spec = importlib.util.spec_from_file_location("credential_gate", sys.argv[1])
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
issues = module.scan_tree(root)
for issue in issues:
    print(issue, file=sys.stderr)
raise SystemExit(1 if issues else 0)
'@
    Invoke-Checked $Python @('-c', $scan, (Join-Path $repo 'scripts\scan_secrets.py'), $payload)
    $privateKeys = Get-ChildItem -LiteralPath $payload -Recurse -File |
        Select-String -Pattern '-----BEGIN (RSA |EC |OPENSSH |DSA )?PRIVATE KEY-----' -List
    if ($privateKeys) {
        throw 'A private-key marker was found in the staged package. No ZIP was produced.'
    }

    $stream = [IO.File]::Open($zipPath, [IO.FileMode]::CreateNew)
    $archive = [IO.Compression.ZipArchive]::new($stream, [IO.Compression.ZipArchiveMode]::Create)
    try {
        foreach ($file in Get-ChildItem -LiteralPath $payload -Recurse -File | Sort-Object FullName) {
            $entryName = [IO.Path]::GetRelativePath($payload, $file.FullName).Replace('\', '/')
            $null = [IO.Compression.ZipFileExtensions]::CreateEntryFromFile(
                $archive, $file.FullName, $entryName, [IO.Compression.CompressionLevel]::Optimal
            )
        }
    } finally {
        $archive.Dispose()
        $stream.Dispose()
    }

    $check = [IO.Compression.ZipFile]::OpenRead($zipPath)
    try {
        $names = @($check.Entries | ForEach-Object { $_.FullName })
        foreach ($required in @(
            'requirements.txt', 'pyproject.toml', 'README.md', 'LICENSE',
            'src/triage/command_center/api.py', 'command-center/dist/index.html'
        )) {
            if ($required -cnotin $names) { throw "Required archive entry is missing: $required" }
        }
        foreach ($name in $names) {
            if ($name.Contains('\') -or $name.StartsWith('/') -or $name -match '(^|/)\.\.(/|$)' -or (Test-ExcludedPath $name)) {
                throw "Unsafe archive entry: $name"
            }
            if ($name -notmatch '^(src/|scenarios/|mock/|command-center/dist/|requirements\.txt$|pyproject\.toml$|README\.md$|LICENSE$)') {
                throw "Unexpected archive entry: $name"
            }
        }
        if (-not ($names -cmatch '^scenarios/[^/]+\.ya?ml$') -or -not ($names -cmatch '^mock/.+')) {
            throw 'The canonical scenarios or mock inputs are absent from the archive.'
        }
        if (-not ($names -match '(?i)^command-center/dist/.*DejaVu[^/]*\.(woff2?|ttf|otf)$')) {
            throw 'The built frontend must include its self-hosted DejaVu fonts.'
        }
        $requirementsReader = [IO.StreamReader]::new($check.GetEntry('requirements.txt').Open())
        try {
            if ($requirementsReader.ReadToEnd().Trim() -cne '.[web,azure]') {
                throw 'The archive requirements must install .[web,azure].'
            }
        } finally {
            $requirementsReader.Dispose()
        }
    } finally {
        $check.Dispose()
    }

    $null = New-Item -ItemType Directory -Path ([IO.Path]::GetDirectoryName($destination)) -Force
    Move-Item -LiteralPath $zipPath -Destination $destination -Force:$Force
    $hash = (Get-FileHash -LiteralPath $destination -Algorithm SHA256).Hash
    Write-Output "Package: $destination"
    Write-Output "SHA256: $hash"
    Write-Output "Entries: $($names.Count)"
} finally {
    # $stage is this invocation's freshly-created, absolute GUID directory.
    Remove-Item -LiteralPath $stage -Recurse -Force
}
