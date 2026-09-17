targetScope = 'resourceGroup'

// Public-network prerequisites only: a dedicated identity and keyless registry.
// No VNet, subnet, NAT gateway or private endpoint is required.
// https://learn.microsoft.com/azure/templates/microsoft.containerregistry/2025-11-01/registries

@sealed()
type GovernanceTags = {
  CostCenter: string
  Owner: string
  Environment: string
  DataClassification: string
}

@sealed()
type RegistryExceptionTags = {
  SecurityControl: 'Ignore'
  ExceptionReason: string
  ExceptionReviewAfter: string
}

param location string
param tags GovernanceTags

@description('Name of the new dedicated monitoring UAMI, not an existing web or controller identity.')
param workerIdentityName string

@description('New globally available registry name, checked by the deployment owner. Lowercase letters and digits only.')
@minLength(5)
@maxLength(50)
param registryName string

@description('Optional, reviewed registry-only exception tags. Leave null unless an observed control blocks the approved public registry access; supply a reason and review date. No Fabric setting is changed.')
param registryExceptionTags RegistryExceptionTags?

resource workerIdentity 'Microsoft.ManagedIdentity/userAssignedIdentities@2024-11-30' = {
  name: workerIdentityName
  location: location
  tags: tags
}

resource registry 'Microsoft.ContainerRegistry/registries@2025-11-01' = {
  name: registryName
  location: location
  tags: union(registryExceptionTags ?? {}, tags)
  sku: {
    name: 'Basic'
  }
  properties: {
    adminUserEnabled: false
    anonymousPullEnabled: false
    publicNetworkAccess: 'Enabled'
    // AcrPull is a registry-RBAC role, not an ABAC repository role.
    roleAssignmentMode: 'LegacyRegistryPermissions'
    policies: {
      azureADAuthenticationAsArmPolicy: {
        status: 'enabled'
      }
    }
  }
}

var acrPullRoleId = subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '7f951dda-4ed3-4680-a7ca-43fe172d538d')

resource imagePull 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(registry.id, workerIdentity.id, acrPullRoleId)
  scope: registry
  properties: {
    principalId: workerIdentity.properties.principalId
    principalType: 'ServicePrincipal'
    roleDefinitionId: acrPullRoleId
    description: 'Image pull for the dedicated monitoring worker identity.'
  }
}

output resourceGroupId string = resourceGroup().id
output workerIdentityResourceId string = workerIdentity.id
output workerIdentityClientId string = workerIdentity.properties.clientId
output workerIdentityPrincipalId string = workerIdentity.properties.principalId
output registryResourceId string = registry.id
output registryLoginServer string = registry.properties.loginServer
output acrPullRoleAssignmentId string = imagePull.id
