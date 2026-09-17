targetScope = 'resourceGroup'

import { GovernanceTags } from './monitoring-environment.bicep'

// Public Foundry account/project with Entra-only authentication and scoped roles.
// Tenant-specific network-policy exceptions are explicit deployment inputs.
// https://learn.microsoft.com/azure/foundry/how-to/create-projects

@description('Globally unique AIServices account/custom subdomain for the accelerator.')
@minLength(2)
@maxLength(54)
param accountName string

@description('Explicit Foundry/model-supported region, checked before deployment.')
@minLength(1)
param location string

@description('New project name within the new account.')
@minLength(2)
@maxLength(64)
param projectName string

param tags GovernanceTags

@description('Verified Entra object ID of the deployment operator. No directory administration is granted.')
@minLength(36)
@maxLength(36)
param operatorObjectId string

@description('Verified object ID of the existing dedicated web managed identity. This is not a Command Center app-role ID.')
@minLength(36)
@maxLength(36)
param webIdentityObjectId string

@description('Optional reviewed network-policy exception tags on this account only. Empty for ordinary deployments; tenant-specific exceptions can expire.')
param accountNetworkExceptionTags object = {}

@description('Existing workspace for account metrics only. No request/response or prompt-content logging is enabled here.')
@minLength(1)
param logAnalyticsWorkspaceResourceId string

@description('Explicit model deployment name. Register the role prompts after endpoint and identity access are verified.')
@minLength(1)
param modelDeploymentName string

@description('Existing, verified agent-compatible OpenAI model to reproduce in the new account; no model-name default.')
@minLength(1)
param modelName string

@description('Pinned model version verified against regional availability before deployment.')
@minLength(1)
param modelVersion string

@description('GlobalStandard deployment capacity units, not a runtime CPU size. Recheck shared regional quota before creation.')
@minValue(1)
param modelCapacity int

var foundryUserRoleId = '53ca6127-db72-4b80-b1b0-d745d6d5456d'

resource account 'Microsoft.CognitiveServices/accounts@2025-10-01-preview' = {
  name: accountName
  location: location
  tags: union(accountNetworkExceptionTags, tags)
  kind: 'AIServices'
  sku: {
    name: 'S0'
  }
  identity: {
    type: 'SystemAssigned'
  }
  properties: {
    allowProjectManagement: true
    customSubDomainName: accountName
    disableLocalAuth: true
    publicNetworkAccess: 'Enabled'
    networkAcls: {
      defaultAction: 'Allow'
      bypass: 'None'
      ipRules: []
      virtualNetworkRules: []
    }
  }
}

resource project 'Microsoft.CognitiveServices/accounts/projects@2025-06-01' = {
  parent: account
  name: projectName
  location: location
  tags: tags
  identity: {
    type: 'SystemAssigned'
  }
  properties: {
    displayName: projectName
    description: 'Hosted triage controller and reasoning agents.'
  }
}

resource modelDeployment 'Microsoft.CognitiveServices/accounts/deployments@2025-06-01' = {
  parent: account
  name: modelDeploymentName
  tags: tags
  sku: {
    name: 'GlobalStandard'
    capacity: modelCapacity
  }
  properties: {
    model: {
      format: 'OpenAI'
      name: modelName
      version: modelVersion
    }
    raiPolicyName: 'Microsoft.DefaultV2'
    versionUpgradeOption: 'NoAutoUpgrade'
  }
}

// Account-scoped access is needed for the project's model proxy. CLI/IaC
// creation must not assume the portal's automatic Foundry User assignments.
resource projectModelAccess 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(account.id, project.id, foundryUserRoleId)
  scope: account
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', foundryUserRoleId)
    principalId: project.identity.principalId
    principalType: 'ServicePrincipal'
  }
}

resource operatorProjectAccess 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(project.id, operatorObjectId, foundryUserRoleId)
  scope: project
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', foundryUserRoleId)
    principalId: operatorObjectId
    principalType: 'User'
  }
}

// The web calls the project Responses API, not only an agent endpoint.
// Agent Consumer is therefore not a substitute for its existing User contract.
resource webProjectAccess 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(project.id, webIdentityObjectId, foundryUserRoleId)
  scope: project
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', foundryUserRoleId)
    principalId: webIdentityObjectId
    principalType: 'ServicePrincipal'
  }
}

// Diagnostic settings and role assignments do not support resource tags.
resource accountMetrics 'Microsoft.Insights/diagnosticSettings@2021-05-01-preview' = {
  name: '${accountName}-metrics'
  scope: account
  properties: {
    workspaceId: logAnalyticsWorkspaceResourceId
    metrics: [
      {
        category: 'AllMetrics'
        enabled: true
      }
    ]
  }
}

output accountResourceId string = account.id
output projectResourceId string = project.id
output projectEndpoint string = project.properties.endpoints['AI Foundry API']
output accountIdentityObjectId string = account.identity.principalId
output projectIdentityObjectId string = project.identity.principalId
output modelDeploymentResourceId string = modelDeployment.id

@description('This foundation does not create a hosted agent, change the old controller, grant SQL data access, upgrade ACR or prove outbound connectivity.')
output networkAndRuntimeAcceptanceRequired bool = true
