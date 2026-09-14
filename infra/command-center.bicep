targetScope = 'resourceGroup'

// Independent of the Foundry deployment and the existing Fabric SQL database.
// Inbound Private Link and outbound VNet integration use different subnets:
// https://learn.microsoft.com/azure/app-service/overview-private-endpoint
// https://learn.microsoft.com/azure/app-service/overview-vnet-integration

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

@description('Choose a SKU with available regional quota. Basic supports Linux VNet integration and private endpoints. NAT and Private Link are billed separately.')
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

@description('Additional non-secret application settings, including FABRIC_SQL_SERVER, FABRIC_SQL_DATABASE and Foundry configuration. Managed settings below take precedence.')
@secure()
param applicationSettings object = {}

param vnetAddressPrefix string = '10.74.0.0/16'
param integrationSubnetPrefix string = '10.74.0.0/26'
param privateEndpointSubnetPrefix string = '10.74.1.0/27'

@description('Preserve an existing subnet NSG, including one attached by governance. The deployment helper reads this binding before redeploying.')
param integrationSubnetNsgId string = ''
param privateEndpointSubnetNsgId string = ''

@description('Optional custom DNS servers. The deployment operator must arrange DNS/peering to existing private Foundry and Fabric endpoints.')
param dnsServers array = []

@description('Optional existing Application Insights resource ID for the portal link. This template neither provisions telemetry nor grants monitoring permissions.')
param applicationInsightsResourceId string = ''

@description('Enable admin-only isolated synthetic scenario validation for an evaluation, never normal production operation.')
param enableEvaluation bool = false

@description('Opt-in evaluation cost-control exemption. Requires an expiry review; the tag does not expire automatically.')
param evaluationCostExemption bool = false

@description('Evaluation review date, YYYY-MM-DD. EvaluationExpiresOn is informational: remove CostControl=Ignore and disable validation before this date.')
param evaluationExpiresOn string = ''

@description('Temporary deployment/verification only. Default is private. The caller must restore Disabled; deploy_command_center.ps1 does so in finally.')
param enablePublicAccess bool = false

@description('Explicit temporary client /32 or /128 CIDRs. Public and SCM access otherwise deny by default, including when this list is empty.')
param publicAccessClientCidrs array = []

var baseTags = union(tags, enableEvaluation ? { EvaluationExpiresOn: evaluationExpiresOn } : {})
var costTags = union(baseTags, evaluationCostExemption ? { CostControl: 'Ignore' } : {})
var appTags = union(costTags, empty(applicationInsightsResourceId) ? {} : {
  'hidden-link:${applicationInsightsResourceId}': 'Resource'
})
var planTier = startsWith(planSku, 'B') ? 'Basic' : (startsWith(planSku, 'S') ? 'Standard' : 'PremiumV3')
var integrationSubnetName = 'app-integration'
var endpointSubnetName = 'private-endpoints'
var integrationSubnetId = resourceId('Microsoft.Network/virtualNetworks/subnets', network.name, integrationSubnetName)
var endpointSubnetId = resourceId('Microsoft.Network/virtualNetworks/subnets', network.name, endpointSubnetName)
var temporaryAccessRules = [for (cidr, i) in publicAccessClientCidrs: {
  name: 'temporary-client-${i}'
  action: 'Allow'
  priority: 100 + i
  ipAddress: cidr
  description: 'Temporary single-host deployment or evaluation access.'
}]

resource identity 'Microsoft.ManagedIdentity/userAssignedIdentities@2023-01-31' = {
  name: '${appName}-identity'
  location: location
  tags: costTags
}

// This IP provides outbound SNAT only, not a public listener. Entra, package
// repositories, and public control-plane endpoints need explicit egress because
// defaultOutboundAccess is disabled. Private destinations still need DNS/routes.
resource outboundIp 'Microsoft.Network/publicIPAddresses@2024-05-01' = {
  name: '${appName}-egress'
  location: location
  tags: costTags
  sku: {
    name: 'Standard'
  }
  properties: {
    publicIPAllocationMethod: 'Static'
    publicIPAddressVersion: 'IPv4'
  }
}

resource nat 'Microsoft.Network/natGateways@2024-05-01' = {
  name: '${appName}-nat'
  location: location
  tags: costTags
  sku: {
    name: 'Standard'
  }
  properties: {
    idleTimeoutInMinutes: 10
    publicIpAddresses: [
      {
        id: outboundIp.id
      }
    ]
  }
}

resource network 'Microsoft.Network/virtualNetworks@2024-05-01' = {
  name: '${appName}-vnet'
  location: location
  tags: costTags
  properties: {
    addressSpace: {
      addressPrefixes: [
        vnetAddressPrefix
      ]
    }
    dhcpOptions: {
      dnsServers: dnsServers
    }
    subnets: [
      {
        name: integrationSubnetName
        properties: union({
          addressPrefix: integrationSubnetPrefix
          defaultOutboundAccess: false
          natGateway: {
            id: nat.id
          }
          delegations: [
            {
              name: 'app-service'
              properties: {
                serviceName: 'Microsoft.Web/serverFarms'
              }
            }
          ]
        }, empty(integrationSubnetNsgId) ? {} : {
          networkSecurityGroup: {
            id: integrationSubnetNsgId
          }
        })
      }
      {
        name: endpointSubnetName
        properties: union({
          addressPrefix: privateEndpointSubnetPrefix
          defaultOutboundAccess: false
          privateEndpointNetworkPolicies: 'Disabled'
        }, empty(privateEndpointSubnetNsgId) ? {} : {
          networkSecurityGroup: {
            id: privateEndpointSubnetNsgId
          }
        })
      }
    ]
  }
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
    publicNetworkAccess: enablePublicAccess ? 'Enabled' : 'Disabled'
    clientAffinityEnabled: false
    virtualNetworkSubnetId: integrationSubnetId
    // The legacy inline siteConfig flag returned false after provisioning.
    // Use the current site-level setting and verify it in ARM after deployment.
    outboundVnetRouting: {
      allTraffic: true
    }
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
      ipSecurityRestrictions: temporaryAccessRules
      ipSecurityRestrictionsDefaultAction: 'Deny'
      scmIpSecurityRestrictionsUseMain: true
      scmIpSecurityRestrictionsDefaultAction: 'Deny'
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

resource privateEndpoint 'Microsoft.Network/privateEndpoints@2024-05-01' = {
  name: '${appName}-endpoint'
  location: location
  tags: costTags
  properties: {
    subnet: {
      id: endpointSubnetId
    }
    privateLinkServiceConnections: [
      {
        name: '${appName}-site'
        properties: {
          privateLinkServiceId: webApp.id
          groupIds: [
            'sites'
          ]
        }
      }
    ]
  }
}

resource privateDns 'Microsoft.Network/privateDnsZones@2024-06-01' = {
  name: 'privatelink.azurewebsites.net'
  location: 'global'
  tags: costTags
}

resource privateDnsLink 'Microsoft.Network/privateDnsZones/virtualNetworkLinks@2024-06-01' = {
  parent: privateDns
  name: '${appName}-link'
  location: 'global'
  tags: costTags
  properties: {
    registrationEnabled: false
    virtualNetwork: {
      id: network.id
    }
  }
}

// The zone group creates both app and SCM records.
resource privateDnsGroup 'Microsoft.Network/privateEndpoints/privateDnsZoneGroups@2024-05-01' = {
  parent: privateEndpoint
  name: 'websites'
  properties: {
    privateDnsZoneConfigs: [
      {
        name: 'websites'
        properties: {
          privateDnsZoneId: privateDns.id
        }
      }
    ]
  }
}

output webAppName string = webApp.name
output webAppId string = webApp.id
output webAppUrl string = 'https://${webApp.properties.defaultHostName}'
output scmUrl string = 'https://${replace(webApp.properties.defaultHostName, '.azurewebsites.net', '.scm.azurewebsites.net')}'
output managedIdentityId string = identity.id
output managedIdentityPrincipalId string = identity.properties.principalId
output managedIdentityClientId string = identity.properties.clientId
output virtualNetworkId string = network.id
output integrationSubnetResourceId string = integrationSubnetId
output privateEndpointSubnetResourceId string = endpointSubnetId
output privateEndpointId string = privateEndpoint.id
output privateDnsZoneId string = privateDns.id
output natGatewayId string = nat.id
output outboundPublicIp string = outboundIp.properties.ipAddress
