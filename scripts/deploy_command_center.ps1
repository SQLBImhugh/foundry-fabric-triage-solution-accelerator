#Requires -Version 7.2
<#
.SYNOPSIS
Validate, or explicitly deploy, the independent command-center App Service.
.DESCRIPTION
Without -Deploy this script runs ARM validation and resource-ID-only what-if.
The resource group must already exist. -Deploy also merges the four governance
tags onto that group, provisions infrastructure, builds the frontend package,
and deploys it with Entra-authenticated ZIP/Oryx deployment (Azure CLI >=2.48.1).
Select -PlanSku using available regional quota; P0v3 is supported when Basic
capacity is unavailable. This script does not request quota increases.

Use -Deploy -ProvisionOnly first when the UAMI still needs Fabric workspace,
database-user and Foundry permissions. Grant those separately, then invoke
-Deploy without -ProvisionOnly. This script never creates SQL logins, changes
Fabric tenant settings, registers Foundry agents, or grants directory/RBAC roles.

ApplicationSettingsFile is an operator-owned JSON object of string values, NOT
an .env file. It must contain AZURE_SQL_SERVER (hostname), AZURE_SQL_DATABASE
(catalog), and FOUNDRY_PROJECT_ENDPOINT. It may include model/agent names, table
names and the other backend settings. Credentials and overrides of managed
authentication/build settings are rejected. Do not commit this input file.

The app and SCM use public HTTPS networking. Entra app roles protect the API;
Entra/Azure RBAC protects deployment. Optional -PublicAccessClientCidr and
-ScmAccessClientCidr restrict their respective network endpoints. Empty lists
leave public reachability enabled. No basic publishing credentials are used
and the helper never switches the application back to private networking.

-Evaluation enables admin-only isolated synthetic validation. The optional
-EvaluationCostExemption applies CostControl=Ignore only to this deployment's
resources. EvaluationExpiresOn is a review tag, not an enforcement mechanism.
Tenant exemptions can expire independently; MCAPS has a single 14-day tag
period that reapplying the tag does not extend.

Existing App Insights is optional; its resource ID links the portal view only.
Managed-identity telemetry authorization and endpoint configuration,
access to Foundry/Fabric, role grants, and post-deployment scenario evidence
remain separate operator tasks. The web app health endpoint is not proof of
database or Foundry connectivity.
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory)]
    [ValidatePattern('^[A-Za-z0-9][A-Za-z0-9 ._-]{0,127}$')]
    [string]$Subscription,
    [Parameter(Mandatory)]
    [ValidatePattern('^[A-Za-z0-9_.-]{1,90}$')]
    [string]$ResourceGroup,
    [Parameter(Mandatory)]
    [ValidatePattern('^[a-z0-9]+$')]
    [string]$Location,
    [Parameter(Mandatory)]
    [ValidatePattern('^[a-zA-Z0-9][a-zA-Z0-9-]{0,38}[a-zA-Z0-9]$')]
    [string]$AppName,
    [Parameter(Mandatory)]
    [guid]$TenantId,
    [Parameter(Mandatory)]
    [guid]$ApplicationClientId,
    [Parameter(Mandatory)]
    [ValidateNotNullOrEmpty()]
    [string]$CostCenter,
    [Parameter(Mandatory)]
    [ValidateNotNullOrEmpty()]
    [string]$Owner,
    [Parameter(Mandatory)]
    [ValidateNotNullOrEmpty()]
    [string]$Environment,
    [Parameter(Mandatory)]
    [ValidateNotNullOrEmpty()]
    [string]$DataClassification,
    [Parameter(Mandatory)]
    [string]$ApplicationSettingsFile,
    [ValidateSet('B1', 'B2', 'B3', 'S1', 'S2', 'S3', 'P0v3', 'P1v3')]
    [string]$PlanSku = 'B1',
    [string]$ApplicationInsightsResourceId = '',
    [switch]$Evaluation,
    [switch]$EvaluationCostExemption,
    [string]$EvaluationExpiresOn = '',
    [string[]]$PublicAccessClientCidr = @(),
    [string[]]$ScmAccessClientCidr = @(),
    [string]$PackageOutputPath,
    [string]$Python,
    [switch]$RestoreDependencies,
    [switch]$ValidateOnly,
    [switch]$Deploy,
    [switch]$ProvisionOnly
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
if ($TenantId -eq [guid]::Empty -or $ApplicationClientId -eq [guid]::Empty) {
    throw 'TenantId and ApplicationClientId must identify the target directory and registered SPA/API.'
}
foreach ($tagValue in @($CostCenter, $Owner, $Environment, $DataClassification)) {
    if ($tagValue -notmatch '^[A-Za-z0-9 @._:/+-]{1,256}$' -or [string]::IsNullOrWhiteSpace($tagValue)) {
        throw 'Governance tags must be nonblank simple labels or email addresses without shell metacharacters.'
    }
}
if ($ValidateOnly -and $Deploy) { throw 'Use either -ValidateOnly or -Deploy, not both.' }
if ($ProvisionOnly -and -not $Deploy) { throw '-ProvisionOnly requires the explicit -Deploy switch.' }
if ($EvaluationCostExemption -and -not $Evaluation) {
    throw 'The cost-control exemption is only supported for an explicit evaluation.'
}
if ($Evaluation) {
    $expiry = [datetime]::MinValue
    if (-not [datetime]::TryParseExact(
        $EvaluationExpiresOn, 'yyyy-MM-dd', [Globalization.CultureInfo]::InvariantCulture,
        [Globalization.DateTimeStyles]::None, [ref]$expiry
    ) -or $expiry.Date -le [datetime]::UtcNow.Date) {
        throw 'Evaluation requires a future EvaluationExpiresOn date in YYYY-MM-DD form.'
    }
}
foreach ($cidr in @($PublicAccessClientCidr) + @($ScmAccessClientCidr)) {
    $parts = $cidr.Split('/')
    $address = [Net.IPAddress]::None
    if ($parts.Count -ne 2 -or -not [Net.IPAddress]::TryParse($parts[0], [ref]$address)) {
        throw 'Each client CIDR must be one explicit IP address.'
    }
    $prefix = if ($address.AddressFamily -eq [Net.Sockets.AddressFamily]::InterNetwork) { '32' } else { '128' }
    if ($parts[1] -cne $prefix) {
        throw 'Client restrictions accept only single-host /32 or /128 CIDRs, never a wildcard.'
    }
}
if ($ApplicationInsightsResourceId -and $ApplicationInsightsResourceId -notmatch '^/subscriptions/[0-9a-f-]{36}/resourceGroups/[^/]+/providers/Microsoft\.Insights/components/[^/]+$') {
    throw 'ApplicationInsightsResourceId must identify an existing Microsoft.Insights/components resource.'
}

$settingsPath = (Resolve-Path -LiteralPath $ApplicationSettingsFile).Path
$inputSettings = Get-Content -LiteralPath $settingsPath -Raw | ConvertFrom-Json -AsHashtable
if ($inputSettings -isnot [Collections.IDictionary]) {
    throw 'ApplicationSettingsFile must contain a JSON object with string values.'
}
$managedKeys = @(
    'COMMAND_CENTER_MODE', 'COMMAND_CENTER_TENANT_ID', 'COMMAND_CENTER_CLIENT_ID',
    'COMMAND_CENTER_SCOPE', 'COMMAND_CENTER_STATIC_DIR', 'COMMAND_CENTER_URL',
    'COMMAND_CENTER_VALIDATION_ENABLED', 'RUN_HISTORY_ENABLED', 'APPROVAL_DELIVERY_MODE',
    'NOTIFICATION_CHANNEL', 'TRIAGE_PROVIDER_MODE', 'TRIAGE_TOOL_MODE',
    'AZURE_CLIENT_ID', 'AZURE_TENANT_ID', 'SCM_DO_BUILD_DURING_DEPLOYMENT', 'ENABLE_ORYX_BUILD',
    'PYTHONPATH', 'PYTHONUNBUFFERED', 'WEBSITE_RUN_FROM_PACKAGE',
    'SCM_SCRIPT_GENERATOR_ARGS', 'PRE_BUILD_COMMAND', 'POST_BUILD_COMMAND'
)
$appSettings = @{}
foreach ($key in $inputSettings.Keys) {
    $name = $key.ToUpperInvariant()
    if ($name -notmatch '^[A-Z][A-Z0-9_]*$' -or $appSettings.ContainsKey($name)) {
        throw "Invalid or case-duplicated application setting name: $key"
    }
    if ($name -in $managedKeys) { throw "Setting $name is controlled by the deployment parameters." }
    if ($name -match '(SECRET|PASSWORD|API_KEY|ACCOUNT_KEY|SAS_TOKEN|CONNECTION_STRING|WEBHOOK_URL|CALLBACK_URL)') {
        throw "Setting $name is not accepted: deploy with managed identity and host/endpoint configuration, not credentials."
    }
    $value = $inputSettings[$key]
    if ($value -isnot [string]) { throw "Setting $name must have a JSON string value." }
    if ($value -match '(?i)(AccountKey|SharedAccessKey|SharedAccessSignature|Password)\s*=' -or $value -match '-----BEGIN .*PRIVATE KEY-----') {
        throw "Setting $name contains credential-shaped content."
    }
    $appSettings[$name] = $value
}
foreach ($required in @('AZURE_SQL_SERVER', 'AZURE_SQL_DATABASE', 'FOUNDRY_PROJECT_ENDPOINT')) {
    if (-not $appSettings.ContainsKey($required) -or [string]::IsNullOrWhiteSpace($appSettings[$required])) {
        throw "The settings file must provide $required for the live application."
    }
}
if ($appSettings.AZURE_SQL_SERVER -notmatch '^[A-Za-z0-9][A-Za-z0-9.-]+$') {
    throw 'AZURE_SQL_SERVER must be a hostname without a protocol, port, or connection-string fields.'
}
if ($appSettings.AZURE_SQL_DATABASE -match '[;=\r\n]') {
    throw 'AZURE_SQL_DATABASE must be a catalog name, not connection-string fields.'
}
$foundryUri = $null
if (-not [uri]::TryCreate($appSettings.FOUNDRY_PROJECT_ENDPOINT, [UriKind]::Absolute, [ref]$foundryUri) -or
    $foundryUri.Scheme -ne 'https' -or $foundryUri.UserInfo -or $foundryUri.Query -or $foundryUri.Fragment) {
    throw 'FOUNDRY_PROJECT_ENDPOINT must be a credential-free HTTPS project endpoint.'
}

function Invoke-AzJson {
    param([string[]]$Arguments)
    $lines = @(& az @Arguments --only-show-errors --output json)
    if ($LASTEXITCODE -ne 0) {
        $operation = ($Arguments | Select-Object -First 2) -join ' '
        throw "Azure CLI $operation failed with exit code $LASTEXITCODE."
    }
    $text = $lines -join [Environment]::NewLine
    if ($text.Trim()) { return ($text | ConvertFrom-Json -AsHashtable) }
}

function Confirm-AzureContext {
    $null = Invoke-AzJson @('account', 'set', '--subscription', $Subscription)
    $account = Invoke-AzJson @('account', 'show', '--subscription', $Subscription)
    if ($account.tenantId -ine $TenantId.ToString()) {
        throw 'The selected subscription is in another tenant. No further operation is permitted.'
    }
    if ($account.environmentName -cne 'AzureCloud') { throw 'This template targets the Azure public cloud only.' }
    return $account
}

function Assert-ManagedAccessContinuity {
    param([Collections.IDictionary]$CurrentSettings, [Collections.IDictionary]$RequestedSettings)
    $name = 'COMMAND_CENTER_ACCESS_MANAGEMENT_ENABLED'
    $enabled = @('true', '1', 'yes', 'on', 'y', 't')
    $current = ([string]$CurrentSettings[$name]).Trim().ToLowerInvariant()
    $requested = ([string]$RequestedSettings[$name]).Trim().ToLowerInvariant()
    if ($requested -in $enabled) {
        throw 'SQL-managed application permissions are retired. Configure Entra application-role assignments instead of enabling COMMAND_CENTER_ACCESS_MANAGEMENT_ENABLED.'
    }
    if ($current -in $enabled) {
        throw 'Complete and verify the Entra access cutover on the existing app before deploying the Entra-only version. Do not silently discard active SQL authorization.'
    }
}

function Confirm-WebProcess {
    param([string]$BaseUrl)
    $deadline = [datetime]::UtcNow.AddMinutes(3)
    do {
        $response = Invoke-WebRequest -Uri "$BaseUrl/api/health" -SkipHttpErrorCheck -TimeoutSec 20
        if ($response.StatusCode -eq 200) {
            $health = $response.Content | ConvertFrom-Json -AsHashtable
            if ($health.status -cne 'ready' -or $health.mode -cne 'live') {
                throw 'The deployed health endpoint did not confirm a ready live web process.'
            }
            Write-Output 'Live web process answered /api/health. SQL and Foundry verification remain separate.'
            return
        }
        if ($response.StatusCode -notin @(404, 500, 502, 503, 504)) {
            throw "Web startup probe returned HTTP $($response.StatusCode). Check endpoint access or the explicit client allowlist."
        }
        Write-Output "Waiting for the web process: HTTP $($response.StatusCode)."
        Start-Sleep -Seconds 5
    } while ([datetime]::UtcNow -lt $deadline)
    throw 'The deployed web process did not become healthy within three minutes.'
}

$repo = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
$template = Join-Path $repo 'infra\command-center.bicep'
$stage = Join-Path ([IO.Path]::GetTempPath()) ('triage-command-center-deploy-' + [guid]::NewGuid().ToString('N'))
$null = New-Item -ItemType Directory -Path $stage
$failure = $null
$appResourceId = ''
try {
    $version = Invoke-AzJson @('version')
    if ([version]$version['azure-cli'] -lt [version]'2.48.1') {
        throw 'Entra-authenticated ZIP deployment requires Azure CLI 2.48.1 or later.'
    }
    $account = Confirm-AzureContext
    $group = Invoke-AzJson @('group', 'show', '--name', $ResourceGroup, '--subscription', $Subscription)
    if (-not $group.id) { throw 'The target resource group must already exist.' }
    $appResourceId = "$($group.id)/providers/Microsoft.Web/sites/$AppName"
    $runtimes = @(Invoke-AzJson @('webapp', 'list-runtimes', '--os', 'linux'))
    if ('PYTHON:3.13' -cnotin $runtimes) {
        throw 'Python 3.13 is not advertised by App Service. Review the runtime before deploying.'
    }

    $existingApps = @(Invoke-AzJson @(
        'webapp', 'list', '--resource-group', $ResourceGroup, '--subscription', $Subscription
    ))
    if (@($existingApps | Where-Object name -IEQ $AppName).Count -gt 0) {
        $currentSettings = @{}
        foreach ($setting in @(Invoke-AzJson @(
            'webapp', 'config', 'appsettings', 'list', '--name', $AppName,
            '--resource-group', $ResourceGroup, '--subscription', $Subscription
        ))) {
            if ($setting.name -ieq 'COMMAND_CENTER_ACCESS_MANAGEMENT_ENABLED') {
                $currentSettings['COMMAND_CENTER_ACCESS_MANAGEMENT_ENABLED'] = $setting.value
            }
        }
        Assert-ManagedAccessContinuity $currentSettings $appSettings
    }

    $tags = @{
        CostCenter = $CostCenter
        Owner = $Owner
        Environment = $Environment
        DataClassification = $DataClassification
    }
    $values = @{
        appName = $AppName
        location = $Location
        tags = $tags
        planSku = $PlanSku
        tenantId = $TenantId.ToString()
        applicationClientId = $ApplicationClientId.ToString()
        applicationSettings = $appSettings
        applicationInsightsResourceId = $ApplicationInsightsResourceId
        enableEvaluation = [bool]$Evaluation
        evaluationCostExemption = [bool]$EvaluationCostExemption
        evaluationExpiresOn = $EvaluationExpiresOn
        publicAccessClientCidrs = @($PublicAccessClientCidr)
        scmAccessClientCidrs = @($ScmAccessClientCidr)
    }
    $parameters = @{}
    foreach ($key in $values.Keys) { $parameters[$key] = @{ value = $values[$key] } }
    $parameterFile = Join-Path $stage 'parameters.json'
    @{
        '$schema' = 'https://schema.management.azure.com/schemas/2019-04-01/deploymentParameters.json#'
        contentVersion = '1.0.0.0'
        parameters = $parameters
    } | ConvertTo-Json -Depth 30 | Set-Content -LiteralPath $parameterFile -Encoding utf8NoBOM
    $compiled = Join-Path $stage 'command-center.json'
    $null = Invoke-AzJson @('bicep', 'build', '--file', $template, '--outfile', $compiled)
    $deploymentArgs = @(
        '--subscription', $Subscription, '--resource-group', $ResourceGroup,
        '--name', "$AppName-command-center", '--template-file', $compiled, '--parameters', "@$parameterFile"
    )
    $null = Confirm-AzureContext
    $null = Invoke-AzJson (@('deployment', 'group', 'validate') + $deploymentArgs)
    $changes = Invoke-AzJson (@('deployment', 'group', 'what-if') + $deploymentArgs + @(
        '--result-format', 'ResourceIdOnly', '--no-pretty-print'
    ))
    foreach ($change in $changes.changes) { Write-Output "$($change.changeType): $($change.resourceId)" }
    if (-not $Deploy) {
        Write-Output 'Validation and what-if finished. No resources were deployed. Supply -Deploy to execute.'
        return
    }

    if (-not $ProvisionOnly) {
        if (-not $PackageOutputPath) { $PackageOutputPath = Join-Path $stage 'command-center.zip' }
        $packageArgs = @{ OutputPath = $PackageOutputPath; RestoreDependencies = $RestoreDependencies }
        if ($Python) { $packageArgs.Python = $Python }
        & (Join-Path $PSScriptRoot 'package_command_center.ps1') @packageArgs
        $PackageOutputPath = (Resolve-Path -LiteralPath $PackageOutputPath).Path
    }
    $null = Confirm-AzureContext
    $tagArgs = @('tag', 'update', '--subscription', $Subscription, '--resource-id', $group.id, '--operation', 'Merge', '--tags')
    foreach ($key in $tags.Keys) { $tagArgs += "$key=$($tags[$key])" }
    $null = Invoke-AzJson $tagArgs
    $null = Confirm-AzureContext
    $outputs = Invoke-AzJson (@('deployment', 'group', 'create') + $deploymentArgs + @('--query', 'properties.outputs'))
    $publicAccess = Invoke-AzJson @(
        'resource', 'show', '--subscription', $Subscription, '--ids', $appResourceId,
        '--query', 'properties.publicNetworkAccess'
    )
    if ($publicAccess -cne 'Enabled') {
        throw 'The deployed app did not retain public networking. Check the tenant network policy and any explicitly approved test exception.'
    }
    Write-Output ($outputs | ConvertTo-Json -Depth 10)
    if ($ProvisionOnly) {
        Write-Output 'Infrastructure provisioned only. Grant the UAMI access to the existing services before deploying code.'
    } else {
        $null = Confirm-AzureContext
        $null = Invoke-AzJson @(
            'webapp', 'deploy', '--subscription', $Subscription, '--resource-group', $ResourceGroup,
            '--name', $AppName, '--src-path', $PackageOutputPath, '--type', 'zip',
            '--clean', 'true', '--restart', 'true', '--async', 'false',
            '--track-status', 'false', '--timeout', '1800000'
        )
        # Control-plane startup tracking stalled after Kudu completed and the
        # real API served requests. Check the process directly instead.
        Confirm-WebProcess "https://$AppName.azurewebsites.net"
        Write-Output 'ZIP/Oryx deployment finished. Validate authenticated APIs, SQL/Foundry access, and scenarios separately.'
    }
} catch {
    $failure = $_
} finally {
    # Only this invocation's explicitly-created GUID directory is removed.
    Remove-Item -LiteralPath $stage -Recurse -Force
}
if ($failure) { throw $failure }
