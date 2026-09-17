targetScope = 'resourceGroup'

// Environment-only bootstrap, also reused by monitoring-worker.bicep.
// It needs no image, worker identity, Fabric endpoint metadata or SQL settings.
// Public platform networking supplies outbound connectivity without a VNet/NAT.
// https://learn.microsoft.com/azure/container-apps/environment
// https://learn.microsoft.com/azure/container-apps/log-options

@export()
type GovernanceTags = {
  CostCenter: string
  Owner: string
  Environment: string
  DataClassification: string
}

@minLength(2)
@maxLength(60)
param environmentName string

param location string
param tags GovernanceTags

@description('Existing workspace resource ID for keyless Azure Monitor diagnostic routing.')
param logAnalyticsWorkspaceResourceId string

@description('One app-owned diagnostic setting. Existing governance settings are not changed.')
param diagnosticSettingName string = '${environmentName}-monitoring'

@description('Optional reviewed exception tags for this new environment only; empty by default.')
param environmentExceptionTags object = {}

resource environment 'Microsoft.App/managedEnvironments@2025-01-01' = {
  name: environmentName
  location: location
  // The platform propagates these tags to its own managed resource group.
  tags: union(environmentExceptionTags, tags)
  properties: {
    workloadProfiles: [
      {
        name: 'Consumption'
        workloadProfileType: 'Consumption'
      }
    ]
    appLogsConfiguration: {
      destination: 'azure-monitor'
    }
  }
}

// Diagnostic settings are extension resources and do not support resource tags.
resource diagnostics 'Microsoft.Insights/diagnosticSettings@2021-05-01-preview' = {
  name: diagnosticSettingName
  scope: environment
  properties: {
    workspaceId: logAnalyticsWorkspaceResourceId
    logAnalyticsDestinationType: 'Dedicated'
    logs: [
      {
        category: 'ContainerAppConsoleLogs'
        enabled: true
      }
      {
        category: 'ContainerAppSystemLogs'
        enabled: true
      }
    ]
    metrics: [
      {
        category: 'AllMetrics'
        enabled: true
      }
    ]
  }
}

output environmentResourceId string = environment.id
output diagnosticSettingResourceId string = diagnostics.id
output logAnalyticsWorkspaceResourceId string = logAnalyticsWorkspaceResourceId
output workerDeployed bool = false
output eventTransportVerified bool = false
