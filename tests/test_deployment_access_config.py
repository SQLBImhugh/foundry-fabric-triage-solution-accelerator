from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest


def test_entra_only_deployment_rejects_retired_or_uncut_sql_authorization() -> None:
    shell = shutil.which("pwsh")
    if shell is None:
        pytest.skip("PowerShell is required for the deployment-helper test")
    script = Path(__file__).resolve().parents[1] / "scripts" / "deploy_command_center.ps1"
    command = r"""
$ErrorActionPreference = 'Stop'
$tokens = $null
$errors = $null
$ast = [Management.Automation.Language.Parser]::ParseFile($args[0], [ref]$tokens, [ref]$errors)
if ($errors.Count) { throw 'Deployment script has parse errors.' }
$definition = $ast.Find({
    param($node)
    $node -is [Management.Automation.Language.FunctionDefinitionAst] -and
    $node.Name -eq 'Assert-ManagedAccessContinuity'
}, $false)
if (-not $definition) { throw 'Managed-access guard was not found.' }
. ([scriptblock]::Create($definition.Extent.Text))
$name = 'COMMAND_CENTER_ACCESS_MANAGEMENT_ENABLED'
Assert-ManagedAccessContinuity @{} @{}
Assert-ManagedAccessContinuity @{$name='false'} @{$name='false'}
foreach ($requested in @(@{}, @{$name='false'}, @{$name='0'}, @{$name='true'})) {
    $refused = $false
    try { Assert-ManagedAccessContinuity @{$name='true'} $requested }
    catch {
        if ($_.Exception.Message -notlike '*Entra*') { throw }
        $refused = $true
    }
    if (-not $refused) { throw 'A redeployment accepted an unverified authorization cutover.' }
}
try { Assert-ManagedAccessContinuity @{} @{$name='true'}; throw 'retired flag accepted' }
catch { if ($_.Exception.Message -notlike '*SQL-managed application permissions are retired*') { throw } }
Write-Output 'managed-access-guard: passed'
"""
    result = subprocess.run(
        [shell, "-NoProfile", "-NonInteractive", "-Command", f"& {{{command}}}", str(script)],
        capture_output=True, text=True, timeout=30, check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert "managed-access-guard: passed" in result.stdout
