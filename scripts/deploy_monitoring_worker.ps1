#Requires -Version 7.2
<#
.SYNOPSIS
Prepare, preflight, validate or preview the independent monitoring worker.
.DESCRIPTION
Use -EnvironmentOnly for the first bootstrap stage. It requires only subscription,
tenant, resource group/location, EnvironmentName, logging ID and the
four governance tags. It creates no worker, needs no image or Fabric endpoint,
and never attaches an identity to another application. After the finite canary
job has established the Eventstream, pass the resulting environment ID to the
unchanged full-worker parameter set with -ExistingEnvironmentResourceId.

The default Prepare mode compiles Bicep locally with --no-restore and writes
template, parameters and a readiness report into a new/empty OutputDirectory.
It makes no Azure account, resource, Fabric or SQL calls.
Optional BicepPath selects an already-installed standalone compiler; it does not
install tooling or alter the Azure CLI profile.

Preflight verifies only the supplied Azure resource IDs and image digest.
Validate adds ARM validation; WhatIf adds a full-payload incremental preview.
Only -Mode WhatIf -Execute permits deployment. No mode creates resource groups,
identities, role assignments, networks, registries, SQL schemas or Fabric items.
No mode changes the existing web app, SCM restrictions, controller or azure.yaml.

Worker InventoryMode defaults to caller_visible. Select tenant_admin_preview to
enable the collector's admin-workspace/domain adapters and preview Admin Items.
This is an explicit API choice, not a grant or proof that those APIs work. Denied
or incomplete enumeration remains a coverage gap; it never becomes complete
all-tenant inventory, operational telemetry or remediation authority.

Cloud modes require an existing, separately authenticated AzureConfigDirectory.
The script selects and verifies SubscriptionId inside that isolated CLI profile;
it never signs in, copies credential caches or changes the shared az context.

Supply a dedicated UAMI whose image-pull and Fabric/SQL grants are already
verified. Registry admin/anonymous authentication must be disabled; ARM-audience
tokens and public network access must be enabled. A new environment uses public
platform networking without a VNet, subnet, NAT gateway or private endpoint.
An existing environment must use public networking and the Consumption workload
profile, and already route logs through azure-monitor to the supplied workspace.
Reuse does not rewrite its settings, even across resource groups.
A new environment gets an own-named diagnostic setting without deleting other
settings. The existing target resource group must carry the supplied tags.

Use -CollectorOnly for initial inventory and REST polling. It requires no
Eventstream metadata and never starts a receiver or changes connector topology.
After owned event transport is configured, omit it and supply ConnectorBootstrapFile.

ConnectorBootstrapFile is an operator-owned JSON object with exactly these
nonsecret strings: connectorId, workspaceId, eventstreamId, destinationId,
fullyQualifiedNamespace, eventHubName, consumerGroup. It identifies an app-owned
connector, not monitored targets or authority to act. No key retrieval is needed.
connectorId, workspaceId, eventstreamId and destinationId must be nonempty UUIDs.
Do not commit populated parameter files.

The template derives AZURE_SUBSCRIPTION_ID, MONITORING_IDENTITY_OBJECT_ID and
MONITORING_IDENTITY_RESOURCE_ID from the selected subscription/existing UAMI.
The worker checks these bindings, tenant and client ID on its managed-identity
tokens. Normal startup requires the shared EventPersistence operations, a
bootstrapped registry and collector; it never falls back to a transport probe.

For an explicitly owned finite canary, the image also supports:
python -m triage.monitoring.worker --transport-probe --probe-workspace-id <source-workspace-uuid> --probe-item-id <source-item-uuid> --probe-seconds 120
This is a read-only transport check, not SQL acceptance or worker readiness.
Do not leave that finite command as a Container App's always-restarted main
process and mistake repeated canary executions for normal monitoring.

EnvironmentExceptionTagsFile optionally supplies a string-valued JSON tag map
for a NEW environment only, including ExceptionReason and a future
ExceptionReviewAfter (YYYY-MM-DD). These tags are review metadata, not an expiry
mechanism. They never apply to the worker, other Azure resources or Fabric.
There is no default exemption. Public outbound networking does not expose
worker HTTP/TCP ingress.

The image must be built separately for linux/amd64 from Dockerfile.monitoring
and supplied by immutable ACR digest. No build, push or package restore runs here.
minReplicas is 1 (or 2 for a replica canary), maxReplicas is 2, with no backlog
autoscaler. The proposed 0.5 vCPU / 1 GiB allocation still needs measured proof.

ARM success is not hybrid acceptance. The deployment owner must separately
prove quotas/cost, DNS and outbound TLS from the worker, UAMI image pull,
Fabric consumption, SQL schema/DML access, durable heartbeat, two-replica
arbitration and restart recovery. Use the worker's metadata-only heartbeat and
logs; there is deliberately no HTTP health endpoint.

The readiness report distinguishes a deployment attempt, an acknowledged ARM
success and verified infrastructure. A failed or interrupted verification must
not be interpreted as proof that no resource was created.
.LINK
https://learn.microsoft.com/azure/container-apps/environment
.LINK
https://learn.microsoft.com/azure/container-apps/log-options
.LINK
https://learn.microsoft.com/azure/container-apps/managed-identity-image-pull
#>
[CmdletBinding(DefaultParameterSetName = 'Worker')]
param(
    [Parameter(Mandatory)][guid]$SubscriptionId,
    [Parameter(Mandatory)][guid]$TenantId,
    [Parameter(Mandatory)][ValidatePattern('^[A-Za-z0-9_.-]{1,90}$')][string]$ResourceGroup,
    [Parameter(Mandatory)][ValidatePattern('^[a-z0-9]+$')][string]$Location,
    [Parameter(Mandatory, ParameterSetName = 'Worker')][ValidatePattern('^[a-z][a-z0-9-]{0,30}[a-z0-9]$')][string]$WorkerName,
    [Parameter(Mandatory, ParameterSetName = 'Worker')][string]$WorkerIdentityResourceId,
    [Parameter(Mandatory, ParameterSetName = 'Worker')][string]$RegistryResourceId,
    [Parameter(Mandatory, ParameterSetName = 'Worker')][string]$Image,
    [Parameter(ParameterSetName = 'Worker')]
    [Parameter(Mandatory, ParameterSetName = 'EnvironmentOnly')]
    [AllowEmptyString()]
    [string]$EnvironmentName = '',
    [Parameter(ParameterSetName = 'Worker')]
    [string]$ExistingEnvironmentResourceId = '',
    [Parameter(Mandatory, ParameterSetName = 'EnvironmentOnly')]
    [switch]$EnvironmentOnly,
    [Parameter(Mandatory)][string]$LogAnalyticsWorkspaceResourceId,
    [Parameter(Mandatory, ParameterSetName = 'Worker')][string]$AzureSqlServer,
    [Parameter(Mandatory, ParameterSetName = 'Worker')][ValidatePattern('^[A-Za-z0-9][A-Za-z0-9 ._()-]{0,127}$')][string]$AzureSqlDatabase,
    [Parameter(ParameterSetName = 'Worker')][string]$ConnectorBootstrapFile = '',
    [Parameter(ParameterSetName = 'Worker')][switch]$CollectorOnly,
    [Parameter(Mandatory)][string]$CostCenter,
    [Parameter(Mandatory)][string]$Owner,
    [Parameter(Mandatory)][string]$Environment,
    [Parameter(Mandatory)][string]$DataClassification,
    [string]$EnvironmentExceptionTagsFile,
    [Parameter(ParameterSetName = 'Worker')][ValidateRange(1, 2)][int]$MinReplicas = 1,
    [Parameter(ParameterSetName = 'Worker')][ValidateSet('caller_visible', 'tenant_admin_preview')]
    [string]$InventoryMode = 'caller_visible',
    [Parameter(Mandatory)][string]$OutputDirectory,
    [string]$AzureConfigDirectory,
    [string]$BicepPath,
    [ValidateSet('Prepare', 'Preflight', 'Validate', 'WhatIf')][string]$Mode = 'Prepare',
    [switch]$Execute,
    [ValidateRange(30, 900)][int]$StartupTimeoutSeconds = 300
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$environmentBootstrap = $PSCmdlet.ParameterSetName -ceq 'EnvironmentOnly'
if ($environmentBootstrap -and -not $EnvironmentOnly) {
    throw 'The environment parameter set requires the explicit -EnvironmentOnly switch.'
}

function Assert-Condition {
    param([bool]$Condition, [string]$Message)
    if (-not $Condition) { throw $Message }
}

function Assert-ResourceId {
    param([string]$Id, [string]$ResourceType)
    $typeParts = $ResourceType.Split('/')
    $pattern = '^/subscriptions/' + [regex]::Escape($SubscriptionId.ToString()) +
        '/resourceGroups/[A-Za-z0-9_.-]+/providers/' + [regex]::Escape($typeParts[0])
    foreach ($part in $typeParts[1..($typeParts.Count - 1)]) {
        $pattern += '/' + [regex]::Escape($part) + '/[A-Za-z0-9_.-]+'
    }
    Assert-Condition ($Id -imatch ($pattern + '$')) "Supply a $ResourceType ID in the selected subscription."
}

function Assert-Hostname {
    param([string]$Value, [string]$Label)
    Assert-Condition ($Value.Length -le 253 -and $Value -cmatch '^(?:[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?\.)+[a-zA-Z]{2,63}$') "$Label must be a DNS hostname without scheme, port, path or credentials."
}

function Normalize-AzureLocation {
    param([string]$Value)
    return ([regex]::Replace($Value, '\s+', '')).ToLowerInvariant()
}

function Assert-Tags {
    param([Collections.IDictionary]$Values)
    foreach ($key in $Values.Keys) {
        Assert-Condition ($key -cmatch '^[A-Za-z][A-Za-z0-9_.-]{0,127}$') 'Tag names must be simple labels.'
        $value = $Values[$key]
        Assert-Condition ($value -is [string] -and $value -cmatch '^[A-Za-z0-9 @._:/+()-]{1,256}$' -and -not [string]::IsNullOrWhiteSpace($value)) 'Tag values must be nonblank labels, not commands or credentials.'
    }
}

function Invoke-AzJson {
    param([string[]]$Arguments, [switch]$AllowEmpty)
    $lines = @(& az @Arguments --only-show-errors --output json)
    Assert-Condition ($LASTEXITCODE -eq 0) "Azure CLI $(($Arguments | Select-Object -First 2) -join ' ') failed with exit code $LASTEXITCODE."
    $text = ($lines -join [Environment]::NewLine).Trim()
    if (-not $text -and $AllowEmpty) { return }
    Assert-Condition ([bool]$text) 'Azure CLI returned no JSON evidence.'
    return ($text | ConvertFrom-Json -AsHashtable)
}

function Confirm-AzureContext {
    $null = Invoke-AzJson @('account', 'set', '--subscription', $SubscriptionId.ToString()) -AllowEmpty
    $account = Invoke-AzJson @('account', 'show', '--subscription', $SubscriptionId.ToString())
    Assert-Condition ($account.id -ieq $SubscriptionId.ToString() -and $account.tenantId -ieq $TenantId.ToString()) 'The selected subscription or tenant does not match the deployment parameters.'
    Assert-Condition ($account.environmentName -ceq 'AzureCloud' -and $account.state -ceq 'Enabled') 'An enabled Azure public-cloud subscription is required.'
}

function Get-ArmResource {
    param([string]$Id, [string]$ApiVersion)
    $value = Invoke-AzJson @('rest', '--method', 'get', '--url', "https://management.azure.com${Id}?api-version=$ApiVersion", '--subscription', $SubscriptionId.ToString())
    Assert-Condition ($value.id -ieq $Id) 'ARM returned a different resource than the requested ID.'
    if ($value.Contains('properties') -and $value.properties.Contains('provisioningState')) {
        Assert-Condition ($value.properties.provisioningState -ceq 'Succeeded') "Resource is not provisioned successfully: $Id"
    }
    return $value
}

function Confirm-Environment {
    param([Collections.IDictionary]$Value)
    $properties = $Value.properties
    Assert-Condition ((Normalize-AzureLocation $Value.location) -ceq (Normalize-AzureLocation $Location)) 'The environment and worker must use the same location.'
    if ($properties['vnetConfiguration']) {
        Assert-Condition ($properties.vnetConfiguration['internal'] -ne $true) 'The reused environment must use public networking, not an internal-only endpoint.'
    }
    Assert-Condition ($properties.appLogsConfiguration.destination -ceq 'azure-monitor') 'An existing environment must already use azure-monitor log routing; this script will not change it.'
    Assert-Condition (@($properties.workloadProfiles | Where-Object { $_.name -ceq 'Consumption' -and $_.workloadProfileType -ceq 'Consumption' }).Count -eq 1) 'A workload-profile environment with the Consumption profile is required.'
}

function Test-LogRouting {
    param([Collections.IDictionary]$Properties)
    if ($Properties['workspaceId'] -ine $LogAnalyticsWorkspaceResourceId) { return $false }
    foreach ($category in @('ContainerAppConsoleLogs', 'ContainerAppSystemLogs')) {
        if (@($Properties['logs'] | Where-Object { $_.enabled -eq $true -and ($_['category'] -ceq $category -or $_['categoryGroup'] -ceq 'allLogs') }).Count -eq 0) {
            return $false
        }
    }
    return $true
}

function Confirm-Prerequisites {
    Confirm-AzureContext
    $group = Invoke-AzJson @('group', 'show', '--name', $ResourceGroup, '--subscription', $SubscriptionId.ToString())
    Assert-Condition ($group.id -ieq $groupId) 'The target resource group must already exist in the selected subscription.'
    foreach ($key in $tags.Keys) {
        Assert-Condition ($group.tags[$key] -ceq $tags[$key]) "The existing resource group must already carry the supplied $key tag; preparation does not retag it."
    }
    $evidence = @{}
    if (-not $environmentBootstrap) {
        $identity = Get-ArmResource $WorkerIdentityResourceId '2023-01-31'
        Assert-Condition ($identity.properties.tenantId -ieq $TenantId.ToString()) 'The worker managed identity belongs to a different tenant.'
        foreach ($key in @('clientId', 'principalId')) {
            $identifier = [guid]::Empty
            Assert-Condition ([guid]::TryParseExact($identity.properties[$key], 'D', [ref]$identifier) -and $identifier -ne [guid]::Empty) "The managed identity has no valid $key."
        }
        $registry = Get-ArmResource $RegistryResourceId '2023-07-01'
        Assert-Condition ($registry.properties.adminUserEnabled -eq $false) 'ACR admin authentication must already be disabled.'
        Assert-Condition ($registry.properties['anonymousPullEnabled'] -eq $false) 'ACR anonymous pull must be explicitly disabled.'
        Assert-Condition ($registry.properties.publicNetworkAccess -ceq 'Enabled') 'The selected registry must expose its public network endpoint; image access still requires managed identity.'
        Assert-Condition ($registry.properties.policies.azureADAuthenticationAsArmPolicy.status -ieq 'enabled') 'ACR must already allow ARM-audience authentication for managed-identity image pull.'
        Assert-Condition ($imageParts[0] -ceq $registry.properties.loginServer) 'The image does not belong to the supplied registry.'
        $manifest = Invoke-AzJson @('acr', 'repository', 'show', '--name', $registry.name, '--image', $imageParts[1], '--subscription', $SubscriptionId.ToString())
        Assert-Condition ($manifest.digest -ceq $imageParts[1].Split('@')[1]) 'ACR did not confirm the requested image digest.'
        $evidence.identityClientId = $identity.properties.clientId
        $evidence.identityPrincipalId = $identity.properties.principalId
        $evidence.registryLoginServer = $registry.properties.loginServer
    }
    $null = Get-ArmResource $LogAnalyticsWorkspaceResourceId '2023-09-01'
    $verifiedDiagnosticId = $diagnosticId
    if ($ExistingEnvironmentResourceId) {
        Confirm-Environment (Get-ArmResource $ExistingEnvironmentResourceId '2025-01-01')
        $settings = Invoke-AzJson @('rest', '--method', 'get', '--url', "https://management.azure.com${environmentId}/providers/Microsoft.Insights/diagnosticSettings?api-version=2021-05-01-preview", '--subscription', $SubscriptionId.ToString())
        $routes = @($settings.value | Where-Object { Test-LogRouting $_.properties })
        Assert-Condition ($routes.Count -gt 0) 'The reused environment needs an existing diagnostic route for console and system logs to the supplied workspace; this script will not change shared logging.'
        $verifiedDiagnosticId = $routes[0].id
        Assert-Condition ($verifiedDiagnosticId -ilike "$environmentId/providers/Microsoft.Insights/diagnosticSettings/*") 'The diagnostic route does not belong to the verified environment.'
    }
    $evidence.diagnosticSettingResourceId = $verifiedDiagnosticId
    return $evidence
}

function Confirm-WhatIf {
    param([Collections.IDictionary]$Preview)
    Assert-Condition ($Preview.status -ceq 'Succeeded' -and $Preview['changes'] -is [array]) 'What-if did not return a successful change manifest.'
    $permitted = @(if ($environmentBootstrap) { $environmentId; $diagnosticId } else { $workerId })
    if (-not $environmentBootstrap -and -not $ExistingEnvironmentResourceId) {
        $permitted += @($environmentId, $diagnosticId, $environmentModuleId)
    }
    foreach ($change in $Preview.changes) {
        Assert-Condition ($change.changeType -cin @('Create', 'Modify', 'NoChange', 'Ignore')) 'What-if contains a deletion or an unverified change type; execution is refused.'
        if ($change.changeType -cin @('Create', 'Modify')) {
            Assert-Condition ($change.resourceId -iin $permitted) 'What-if would modify a resource outside this worker deployment.'
            Assert-Condition (-not $environmentBootstrap -or $change.changeType -cne 'Modify') 'Environment-only bootstrap does not adopt or rewrite an existing environment. Promote through the full worker using ExistingEnvironmentResourceId.'
        }
    }
}

function Confirm-WorkerProperties {
    param([Collections.IDictionary]$App, [Collections.IDictionary]$Evidence)
    $properties = $App.properties
    Assert-Condition ($App.id -ieq $workerId -and (Normalize-AzureLocation $App.location) -ceq (Normalize-AzureLocation $Location)) 'Worker ID or location does not match.'
    Assert-Condition ($properties.environmentId -ieq $environmentId -and $properties.workloadProfileName -ceq 'Consumption') 'Worker environment or workload profile does not match.'
    Assert-Condition ($null -eq $properties.configuration['ingress']) 'The worker must have no HTTP or TCP ingress.'
    Assert-Condition ($properties.configuration.activeRevisionsMode -ceq 'Single') 'The worker must use single-revision mode.'
    Assert-Condition ($App.identity.type -ceq 'UserAssigned' -and $App.identity.userAssignedIdentities.Count -eq 1 -and $App.identity.userAssignedIdentities.Contains($WorkerIdentityResourceId)) 'The worker must use only the selected managed identity.'
    Assert-Condition (@($properties.configuration['secrets'] | Where-Object { $_ }).Count -eq 0) 'Worker secrets are not permitted.'
    $pull = @($properties.configuration.registries)
    Assert-Condition ($pull.Count -eq 1 -and $pull[0].server -ceq $Evidence.registryLoginServer -and $pull[0].identity -ieq $WorkerIdentityResourceId -and -not $pull[0]['username'] -and -not $pull[0]['passwordSecretRef']) 'ACR pull is not bound exclusively to the selected managed identity.'
    $containers = @($properties.template.containers)
    Assert-Condition ($containers.Count -eq 1) 'The worker must contain exactly one container.'
    $container = $containers[0]
    Assert-Condition ($container.name -ceq 'monitoring' -and $container.image -ceq $Image -and ($container.command -join ' ') -ceq 'python -m triage.monitoring.worker') 'Worker image or entry point does not match.'
    $workerArguments = @($container['args'] | Where-Object { $_ })
    Assert-Condition (($workerArguments -join ' ') -ceq $(if ($CollectorOnly) { '--collector-only' } else { '' })) 'Worker collection mode does not match.'
    Assert-Condition ($container.resources.cpu -eq 0.5 -and $container.resources.memory -ceq '1Gi') 'Worker resources must be 0.5 vCPU and 1 GiB.'
    Assert-Condition ($properties.template.scale.minReplicas -eq $MinReplicas -and $properties.template.scale.maxReplicas -eq 2 -and @($properties.template.scale['rules'] | Where-Object { $_ }).Count -eq 0) 'Worker replica bounds or scaling rules do not match.'
    $expected = @{
        AZURE_SUBSCRIPTION_ID = $SubscriptionId.ToString()
        AZURE_TENANT_ID = $TenantId.ToString(); AZURE_CLIENT_ID = $Evidence.identityClientId
        MONITORING_IDENTITY_OBJECT_ID = $Evidence.identityPrincipalId
        MONITORING_IDENTITY_RESOURCE_ID = $WorkerIdentityResourceId
        MONITORING_MODE = 'live'; MONITORING_TENANT_ID = $TenantId.ToString()
        MONITORING_INVENTORY_MODE = $InventoryMode
        AZURE_SQL_SERVER = $AzureSqlServer; AZURE_SQL_DATABASE = $AzureSqlDatabase
        PYTHONUNBUFFERED = '1'; PYTHONDONTWRITEBYTECODE = '1'
    }
    if (-not $CollectorOnly) {
        $expected.MONITORING_CONNECTOR_ID = $bootstrap.connectorId
        $expected.MONITORING_EVENTSTREAM_WORKSPACE_ID = $bootstrap.workspaceId
        $expected.MONITORING_EVENTSTREAM_ID = $bootstrap.eventstreamId
        $expected.MONITORING_EVENTSTREAM_DESTINATION_ID = $bootstrap.destinationId
        $expected.MONITORING_EVENTSTREAM_NAMESPACE = $bootstrap.fullyQualifiedNamespace
        $expected.MONITORING_EVENTSTREAM_ENTITY = $bootstrap.eventHubName
        $expected.MONITORING_EVENTSTREAM_CONSUMER_GROUP = $bootstrap.consumerGroup
    }
    Assert-Condition (@($container.env).Count -eq $expected.Count) 'Unexpected or missing worker environment variables.'
    foreach ($key in $expected.Keys) {
        $setting = @($container.env | Where-Object { $_.name -ceq $key })
        Assert-Condition ($setting.Count -eq 1 -and $setting[0].value -ceq $expected[$key] -and -not $setting[0]['secretRef']) "Worker configuration mismatch: $key"
    }
    foreach ($key in $tags.Keys) {
        Assert-Condition ($App.tags[$key] -ceq $tags[$key]) "Worker tag mismatch: $key"
    }
}

Assert-Condition ($SubscriptionId -ne [guid]::Empty -and $TenantId -ne [guid]::Empty) 'SubscriptionId and TenantId must be nonempty GUIDs.'
Assert-Condition (-not $Execute -or $Mode -ceq 'WhatIf') '-Execute requires -Mode WhatIf.'
Assert-Condition ([bool]$EnvironmentName -xor [bool]$ExistingEnvironmentResourceId) 'Supply exactly one of EnvironmentName or ExistingEnvironmentResourceId.'
if ($EnvironmentName) {
    Assert-Condition ($EnvironmentName -cmatch '^[a-zA-Z][a-zA-Z0-9-]{0,58}[a-zA-Z0-9]$') 'EnvironmentName must be a 2-60 character Azure environment name.'
} else { Assert-ResourceId $ExistingEnvironmentResourceId 'Microsoft.App/managedEnvironments' }
Assert-ResourceId $LogAnalyticsWorkspaceResourceId 'Microsoft.OperationalInsights/workspaces'
$tags = @{ CostCenter = $CostCenter; Owner = $Owner; Environment = $Environment; DataClassification = $DataClassification }
Assert-Tags $tags
if (-not $environmentBootstrap) {
    Assert-Condition (-not $WorkerName.Contains('--')) 'WorkerName cannot contain consecutive hyphens.'
    Assert-ResourceId $WorkerIdentityResourceId 'Microsoft.ManagedIdentity/userAssignedIdentities'
    Assert-ResourceId $RegistryResourceId 'Microsoft.ContainerRegistry/registries'
    Assert-Hostname $AzureSqlServer 'AzureSqlServer'
    Assert-Condition ($Image -cmatch '^[a-z0-9.-]+/[a-z0-9]+(?:[._/-][a-z0-9]+)*@sha256:[0-9a-f]{64}$') 'Image must be a credential-free ACR image pinned by sha256 digest, not a mutable tag.'
    $imageParts = $Image.Split('/', 2)
    Assert-Hostname $imageParts[0] 'Image registry'
    Assert-Condition ([bool]$CollectorOnly -xor [bool]$ConnectorBootstrapFile) 'Select CollectorOnly or supply ConnectorBootstrapFile for event mode, not both.'
    $bootstrap = if ($CollectorOnly) { $null } else { Get-Content -LiteralPath $ConnectorBootstrapFile -Raw | ConvertFrom-Json -AsHashtable }
    if (-not $CollectorOnly) {
        $bootstrapKeys = @('connectorId', 'workspaceId', 'eventstreamId', 'destinationId', 'fullyQualifiedNamespace', 'eventHubName', 'consumerGroup')
        Assert-Condition ($bootstrap -is [Collections.IDictionary] -and $bootstrap.Count -eq $bootstrapKeys.Count) 'ConnectorBootstrapFile must contain exactly the documented nonsecret fields.'
        foreach ($key in $bootstrap.Keys) {
            Assert-Condition ($key -cin $bootstrapKeys -and $bootstrap[$key] -is [string] -and -not [string]::IsNullOrWhiteSpace($bootstrap[$key])) 'Connector bootstrap has an unknown, blank or non-string field.'
        }
        foreach ($key in @('connectorId', 'workspaceId', 'eventstreamId', 'destinationId')) {
            $identifier = [guid]::Empty
            Assert-Condition ([guid]::TryParseExact($bootstrap[$key], 'D', [ref]$identifier) -and $identifier -ne [guid]::Empty) "Connector bootstrap $key must be a nonempty GUID."
        }
        Assert-Hostname $bootstrap.fullyQualifiedNamespace 'fullyQualifiedNamespace'
        Assert-Condition ($bootstrap.eventHubName -cmatch '^[A-Za-z0-9][A-Za-z0-9_.-]{0,255}$') 'eventHubName must be a nonsecret entity name.'
        Assert-Condition ($bootstrap.consumerGroup -cmatch '^[A-Za-z0-9$][A-Za-z0-9$_.-]{0,49}$') 'consumerGroup must be a nonsecret group name.'
    }
}
$exceptionTags = @{}
if ($EnvironmentExceptionTagsFile) {
    Assert-Condition (-not $ExistingEnvironmentResourceId) 'Exception tags cannot modify an existing environment.'
    $exceptionTags = Get-Content -LiteralPath $EnvironmentExceptionTagsFile -Raw | ConvertFrom-Json -AsHashtable
    Assert-Condition ($exceptionTags -is [Collections.IDictionary]) 'EnvironmentExceptionTagsFile must contain a JSON tag map.'
    Assert-Tags $exceptionTags
    foreach ($key in $tags.Keys) {
        Assert-Condition (-not $exceptionTags.Contains($key)) 'Exception tags cannot override governance tags.'
    }
    $review = [datetime]::MinValue
    Assert-Condition ($exceptionTags.Contains('ExceptionReason') -and $exceptionTags.Contains('ExceptionReviewAfter')) 'Exception tags require ExceptionReason and ExceptionReviewAfter.'
    Assert-Condition ([datetime]::TryParseExact($exceptionTags.ExceptionReviewAfter, 'yyyy-MM-dd', [Globalization.CultureInfo]::InvariantCulture, [Globalization.DateTimeStyles]::None, [ref]$review) -and $review.Date -gt [datetime]::UtcNow.Date) 'ExceptionReviewAfter must be a future YYYY-MM-DD date; expiry is not automatic.'
}
if ($Mode -cne 'Prepare') {
    Assert-Condition ([bool]$AzureConfigDirectory -and (Test-Path -LiteralPath $AzureConfigDirectory -PathType Container)) 'Cloud modes require an existing, separately authenticated AzureConfigDirectory.'
    $AzureConfigDirectory = (Resolve-Path -LiteralPath $AzureConfigDirectory).Path
    $sharedConfig = [IO.Path]::GetFullPath((Join-Path $HOME '.azure'))
    Assert-Condition ($AzureConfigDirectory.TrimEnd('\', '/') -ine $sharedConfig.TrimEnd('\', '/')) 'Cloud modes must not use the shared Azure CLI profile.'
}
if ($BicepPath) {
    Assert-Condition (Test-Path -LiteralPath $BicepPath -PathType Leaf) 'BicepPath must name an existing standalone compiler.'
    $BicepPath = (Resolve-Path -LiteralPath $BicepPath).Path
}

$repo = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
$artifact = if ($environmentBootstrap) { 'monitoring-environment' } else { 'monitoring-worker' }
$template = Join-Path $repo "infra\$artifact.bicep"
$output = [IO.Path]::GetFullPath($OutputDirectory)
Assert-Condition (-not (Test-Path -LiteralPath $output) -or ((Test-Path -LiteralPath $output -PathType Container) -and @(Get-ChildItem -LiteralPath $output -Force).Count -eq 0)) 'OutputDirectory must be new or empty; existing artifacts are never overwritten.'
$null = New-Item -ItemType Directory -Path $output -Force
$templateFile = Join-Path $output "$artifact.template.json"
$parameterFile = Join-Path $output "$artifact.parameters.json"
$reportFile = Join-Path $output "$artifact.readiness.json"
$groupId = "/subscriptions/$SubscriptionId/resourceGroups/$ResourceGroup"
$environmentId = if ($ExistingEnvironmentResourceId) { $ExistingEnvironmentResourceId } else { "$groupId/providers/Microsoft.App/managedEnvironments/$EnvironmentName" }
$diagnosticName = if ($environmentBootstrap) { "$EnvironmentName-monitoring" } else { "$WorkerName-monitoring" }
$diagnosticId = if ($ExistingEnvironmentResourceId) { '' } else { "$environmentId/providers/Microsoft.Insights/diagnosticSettings/$diagnosticName" }
$values = @{
    location = $Location; tags = $tags; environmentName = $EnvironmentName
    logAnalyticsWorkspaceResourceId = $LogAnalyticsWorkspaceResourceId
    environmentExceptionTags = $exceptionTags
}
if ($environmentBootstrap) {
    $values.diagnosticSettingName = $diagnosticName
    $deploymentName = "$EnvironmentName-bootstrap"
} else {
    $workerId = "$groupId/providers/Microsoft.App/containerApps/$WorkerName"
    $environmentModuleId = "$groupId/providers/Microsoft.Resources/deployments/$WorkerName-environment"
    $deploymentName = "$WorkerName-preparation"
    $values.workerName = $WorkerName
    $values.tenantId = $TenantId.ToString()
    $values.workerIdentityResourceId = $WorkerIdentityResourceId
    $values.registryResourceId = $RegistryResourceId
    $values.image = $Image
    $values.existingEnvironmentResourceId = $ExistingEnvironmentResourceId
    $values.azureSqlServer = $AzureSqlServer
    $values.azureSqlDatabase = $AzureSqlDatabase
    $values.connectorBootstrap = $bootstrap
    $values.collectorOnly = [bool]$CollectorOnly
    $values.minReplicas = $MinReplicas
    $values.inventoryMode = $InventoryMode
}
$parameters = @{}
foreach ($key in $values.Keys) { $parameters[$key] = @{ value = $values[$key] } }
$report = @{
    status = 'Preparing'; deployed = $false; deploymentAttempted = $false
    deploymentAcknowledged = $false; azurePropertiesVerified = $false; hybridAcceptanceVerified = $false
    artifactKind = $artifact; workerDeployed = $false; environmentResourceId = $environmentId
    mode = $Mode
    remainingGates = @('Regional quota and cost', 'Linux/amd64 image and deployed UAMI image pull',
        'Public endpoint DNS and outbound TLS', 'Fabric ownership and UAMI consumption',
        'SQL schema and limited DML access', 'Durable heartbeat, two-replica arbitration and restart recovery')
}
if (-not $environmentBootstrap) { $report.workerResourceId = $workerId }
$previousAzureConfig = $env:AZURE_CONFIG_DIR
try {
    if ($BicepPath) {
        & $BicepPath build $template --outfile $templateFile --no-restore
        Assert-Condition ($LASTEXITCODE -eq 0) "Bicep build failed with exit code $LASTEXITCODE."
    } else {
        $null = Invoke-AzJson @('bicep', 'build', '--file', $template, '--outfile', $templateFile, '--no-restore') -AllowEmpty
    }
    $compiled = Get-Content -LiteralPath $templateFile -Raw | ConvertFrom-Json -AsHashtable
    Assert-Condition ($compiled.Contains('resources') -and $compiled.Contains('parameters')) 'Bicep did not produce an ARM template.'
    @{ '$schema' = 'https://schema.management.azure.com/schemas/2019-04-01/deploymentParameters.json#'; contentVersion = '1.0.0.0'; parameters = $parameters } |
        ConvertTo-Json -Depth 30 | Set-Content -LiteralPath $parameterFile -Encoding utf8
    $report.status = if ($environmentBootstrap) { 'EnvironmentPrepared' } else { 'Prepared' }
    if ($Mode -cne 'Prepare') {
        $env:AZURE_CONFIG_DIR = $AzureConfigDirectory
        $evidence = Confirm-Prerequisites
        $report.azurePropertiesVerified = $true
        $report.status = 'PreflightPassed'
        $deploymentArgs = @('--resource-group', $ResourceGroup, '--subscription', $SubscriptionId.ToString(),
            '--name', $deploymentName, '--mode', 'Incremental',
            '--template-file', $templateFile, '--parameters', "@$parameterFile")
        if ($Mode -cin @('Validate', 'WhatIf')) {
            $validation = Invoke-AzJson (@('deployment', 'group', 'validate') + $deploymentArgs)
            Assert-Condition ($validation.properties.provisioningState -ceq 'Succeeded') 'ARM validation did not return Succeeded.'
            $report.status = 'Validated'
        }
        if ($Mode -ceq 'WhatIf') {
            $preview = Invoke-AzJson (@('deployment', 'group', 'what-if') + $deploymentArgs + @('--result-format', 'FullResourcePayloads', '--no-pretty-print'))
            Confirm-WhatIf $preview
            $preview | ConvertTo-Json -Depth 100 | Set-Content -LiteralPath (Join-Path $output "$artifact.what-if.json") -Encoding utf8
            $report.status = 'WhatIfPassed'
        }
        if ($Execute) {
            Confirm-AzureContext
            # A lost acknowledgement is not evidence that deployment did not run.
            $report.deploymentAttempted = $true
            $report | ConvertTo-Json -Depth 30 | Set-Content -LiteralPath $reportFile -Encoding utf8
            $deployment = Invoke-AzJson (@('deployment', 'group', 'create') + $deploymentArgs)
            Assert-Condition ($deployment.properties.provisioningState -ceq 'Succeeded') 'Deployment did not return Succeeded.'
            $report.deploymentAcknowledged = $true
            $expectedOutputs = @{
                environmentResourceId = $environmentId; diagnosticSettingResourceId = $diagnosticId
            }
            if ($environmentBootstrap) {
                $expectedOutputs.logAnalyticsWorkspaceResourceId = $LogAnalyticsWorkspaceResourceId
            } else {
                $expectedOutputs.workerResourceId = $workerId
                $expectedOutputs.workerIdentityResourceId = $WorkerIdentityResourceId
                $expectedOutputs.workerIdentityClientId = $evidence.identityClientId
                $expectedOutputs.workerIdentityPrincipalId = $evidence.identityPrincipalId
                $expectedOutputs.registryLoginServer = $evidence.registryLoginServer
                $expectedOutputs.deployedImage = $Image
            }
            foreach ($key in $expectedOutputs.Keys) {
                Assert-Condition ($deployment.properties.outputs[$key].value -ieq $expectedOutputs[$key]) "Deployment output mismatch: $key"
            }
            Confirm-Environment (Get-ArmResource $environmentId '2025-01-01')
            $diagnostics = Get-ArmResource $evidence.diagnosticSettingResourceId '2021-05-01-preview'
            Assert-Condition (Test-LogRouting $diagnostics.properties) 'Console and system diagnostic routing was not verified.'
            if (-not $environmentBootstrap) {
                $deadline = [datetime]::UtcNow.AddSeconds($StartupTimeoutSeconds)
                do {
                    $app = Get-ArmResource $workerId '2025-01-01'
                    Confirm-WorkerProperties $app $evidence
                    $ready = $app.properties.runningStatus -ceq 'Running' -and
                        [bool]$app.properties.latestRevisionName -and
                        $app.properties.latestRevisionName -ceq $app.properties.latestReadyRevisionName
                    if ($ready) { break }
                    Start-Sleep -Seconds 5
                } while ([datetime]::UtcNow -lt $deadline)
                Assert-Condition $ready 'The requested worker revision did not become running and ready before the timeout.'
                $report.workerDeployed = $true
            }
            $report.status = if ($environmentBootstrap) { 'EnvironmentInfrastructureVerified' } else { 'InfrastructureVerified' }
            $report.deployed = $true
        }
    }
} catch {
    $report.status = 'Failed'
    $report.error = $_.Exception.Message
    throw
} finally {
    $env:AZURE_CONFIG_DIR = $previousAzureConfig
    $report | ConvertTo-Json -Depth 30 | Set-Content -LiteralPath $reportFile -Encoding utf8
}
Write-Output "$($report.status). Report: $reportFile. Hybrid acceptance remains a separate live gate."
