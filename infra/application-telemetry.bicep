targetScope = 'resourceGroup'

import { GovernanceTags } from './monitoring-environment.bicep'

param name string
param location string
param tags GovernanceTags
param logAnalyticsWorkspaceResourceId string

@description('Object IDs of the actual telemetry-emitting identities. Empty before those identities exist; assign only this resource-scoped publisher role afterward.')
param publisherPrincipalIds string[] = []

// This resource is not attached as a Foundry project tracing connection.
// Application health must not enable project-wide prompt/content collection.
resource insights 'Microsoft.Insights/components@2020-02-02' = {
  name: name
  location: location
  kind: 'web'
  tags: tags
  properties: {
    Application_Type: 'web'
    WorkspaceResourceId: logAnalyticsWorkspaceResourceId
    IngestionMode: 'LogAnalytics'
    DisableLocalAuth: true
    publicNetworkAccessForIngestion: 'Enabled'
    publicNetworkAccessForQuery: 'Enabled'
  }
}

resource publishers 'Microsoft.Authorization/roleAssignments@2022-04-01' = [for principal in publisherPrincipalIds: {
  name: guid(insights.id, principal, '3913510d-42f4-4e42-8a64-420c390055eb')
  scope: insights
  properties: {
    principalId: principal
    principalType: 'ServicePrincipal'
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '3913510d-42f4-4e42-8a64-420c390055eb')
  }
}]

output applicationInsightsResourceId string = insights.id
