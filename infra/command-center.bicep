targetScope = 'resourceGroup'

// Independent of the Foundry deployment and the existing Azure SQL database.
// Public HTTPS networking; Entra app roles protect the API and Azure RBAC
// protects deployment. No VNet, NAT gateway or private DNS is required.
// https://learn.microsoft.com/azure/app-service/overview-access-restrictions

type GovernanceTags = {
  CostCenter: string
  Owner: string
  Environment: string
  DataClassification: string
}

@description('Globally unique web app name. Related resources use this prefix.')
@minLength(2)
@maxLength(40)
param appName string

param location string = resourceGroup().location
param tags GovernanceTags

@description('Choose an App Service SKU with available regional quota. No additional network resources are provisioned.')
@allowed([
  'B1'
  'B2'
  'B3'
  'S1'
  'S2'
  'S3'
  'P0v3'
  'P1v3'
])
param planSku string = 'B1'

@description('The tenant and the shared SPA/API application client ID; neither is a credential.')
param tenantId string
param applicationClientId string

@description('Additional non-secret application settings, including AZURE_SQL_SERVER, AZURE_SQL_DATABASE and Foundry configuration. Managed settings below take precedence.')
@secure()
param applicationSettings object = {}

@description('Optional existing Application Insights resource ID for the portal link. This template neither provisions telemetry nor grants monitoring permissions.')
param applicationInsightsResourceId string = ''

@description('Enable admin-only isolated synthetic scenario validation for an evaluation, never normal production operation.')
param enableEvaluation bool = false

@description('Opt-in evaluation cost-control exemption. Review tenant limits: MCAPS tag exemptions have a single limited period and reapplying them does not extend it.')
param evaluationCostExemption bool = false

@description('Evaluation review date, YYYY-MM-DD. EvaluationExpiresOn is informational: remove CostControl=Ignore and disable validation before this date.')
param evaluationExpiresOn string = ''

@description('Optional main-site client CIDRs. Empty means public network reachability, not anonymous API authorization.')
param publicAccessClientCidrs array = []

@description('Optional separate deployment-endpoint client CIDRs. Empty permits public SCM networking; Entra deployment authentication remains required.')
param scmAccessClientCidrs array = []

var baseTags = union(tags, enableEvaluation ? { EvaluationExpiresOn: evaluationExpiresOn } : {})
var costTags = union(baseTags, evaluationCostExemption ? { CostControl: 'Ignore' } : {})
var appTags = union(costTags, empty(applicationInsightsResourceId) ? {} : {
  'hidden-link:${applicationInsightsResourceId}': 'Resource'
})
var planTier = startsWith(planSku, 'B') ? 'Basic' : (startsWith(planSku, 'S') ? 'Standard' : 'PremiumV3')
var clientAccessRules = [for (cidr, i) in publicAccessClientCidrs: {
  name: 'client-${i}'
  action: 'Allow'
  priority: 100 + i
  ipAddress: cidr
  description: 'Main-site network admission; application roles still apply.'
}]
var scmAccessRules = [for (cidr, i) in scmAccessClientCidrs: {
  name: 'deployment-client-${i}'
  action: 'Allow'
  priority: 100 + i
  ipAddress: cidr
  description: 'Deployment admission; Entra authentication still applies.'
}]

resource identity 'Microsoft.ManagedIdentity/userAssignedIdentities@2023-01-31' = {
  name: '${appName}-identity'
  location: location
  tags: costTags
}

resource plan 'Microsoft.Web/serverfarms@2024-11-01' = {
  name: '${appName}-plan'
  location: location
  tags: costTags
  kind: 'linux'
  sku: {
    name: planSku
    tier: planTier
    capacity: 1
  }
  properties: {
    reserved: true
  }
}

var managedSettings = {
  COMMAND_CENTER_MODE: 'live'
  COMMAND_CENTER_TENANT_ID: tenantId
  COMMAND_CENTER_CLIENT_ID: applicationClientId
  COMMAND_CENTER_SCOPE: 'api://${applicationClientId}/access_as_user'
  COMMAND_CENTER_STATIC_DIR: 'command-center/dist'
  COMMAND_CENTER_URL: 'https://${appName}.azurewebsites.net'
  COMMAND_CENTER_VALIDATION_ENABLED: enableEvaluation ? 'true' : 'false'
  RUN_HISTORY_ENABLED: 'true'
  APPROVAL_DELIVERY_MODE: 'web'
  NOTIFICATION_CHANNEL: 'web'
  TRIAGE_PROVIDER_MODE: 'foundry'
  TRIAGE_TOOL_MODE: 'live'
  AZURE_CLIENT_ID: identity.properties.clientId
  AZURE_TENANT_ID: tenantId
  SCM_DO_BUILD_DURING_DEPLOYMENT: 'true'
  ENABLE_ORYX_BUILD: 'true'
  // Oryx can relocate the app. Relative src keeps imports and repository-relative
  // scenario paths on the deployed source rather than the installed wheel.
  PYTHONPATH: 'src'
  PYTHONUNBUFFERED: '1'
}
var effectiveSettings = union(applicationSettings, managedSettings)

resource webApp 'Microsoft.Web/sites@2024-11-01' = {
  name: appName
  location: location
  tags: appTags
  kind: 'app,linux'
  identity: {
    type: 'UserAssigned'
    userAssignedIdentities: {
      '${identity.id}': {}
    }
  }
  properties: {
    serverFarmId: plan.id
    httpsOnly: true
    publicNetworkAccess: 'Enabled'
    clientAffinityEnabled: false
    siteConfig: {
      // Verified with az webapp list-runtimes --os linux (2026-09-12).
      linuxFxVersion: 'PYTHON|3.13'
      appCommandLine: 'python -m uvicorn triage.command_center.api:create_app --factory --host 0.0.0.0 --port 8000'
      alwaysOn: true
      healthCheckPath: '/api/health'
      ftpsState: 'Disabled'
      minTlsVersion: '1.2'
      scmMinTlsVersion: '1.2'
      http20Enabled: true
      ipSecurityRestrictions: clientAccessRules
      ipSecurityRestrictionsDefaultAction: empty(publicAccessClientCidrs) ? 'Allow' : 'Deny'
      scmIpSecurityRestrictions: scmAccessRules
      scmIpSecurityRestrictionsUseMain: false
      scmIpSecurityRestrictionsDefaultAction: empty(scmAccessClientCidrs) ? 'Allow' : 'Deny'
      appSettings: map(items(effectiveSettings), setting => {
        name: setting.key
        value: string(setting.value)
      })
    }
  }
}

// These child resource types do not support tags. All taggable parent resources
// carry the required governance tags. Do not enable publishing credentials:
// az webapp deploy uses Entra authentication with Azure CLI >= 2.48.1.
resource scmPublishing 'Microsoft.Web/sites/basicPublishingCredentialsPolicies@2024-11-01' = {
  parent: webApp
  name: 'scm'
  properties: {
    allow: false
  }
}

resource ftpPublishing 'Microsoft.Web/sites/basicPublishingCredentialsPolicies@2024-11-01' = {
  parent: webApp
  name: 'ftp'
  properties: {
    allow: false
  }
}

output webAppName string = webApp.name
output webAppId string = webApp.id
output webAppUrl string = 'https://${webApp.properties.defaultHostName}'
output scmUrl string = 'https://${replace(webApp.properties.defaultHostName, '.azurewebsites.net', '.scm.azurewebsites.net')}'
output managedIdentityId string = identity.id
output managedIdentityPrincipalId string = identity.properties.principalId
output managedIdentityClientId string = identity.properties.clientId
