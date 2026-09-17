targetScope = 'resourceGroup'

import { GovernanceTags } from './monitoring-environment.bicep'

// Independent operator deployment: azure.yaml uses the Foundry provider.
// Identity, registry, SQL and workspace grants are prerequisites, not
// resources this template creates or changes.
// https://learn.microsoft.com/azure/container-apps/environment
// https://learn.microsoft.com/azure/container-apps/log-options
// https://learn.microsoft.com/azure/container-apps/managed-identity-image-pull

@sealed()
type ConnectorBootstrap = {
  connectorId: string
  workspaceId: string
  eventstreamId: string
  destinationId: string
  fullyQualifiedNamespace: string
  eventHubName: string
  consumerGroup: string
}

@description('Container App name, independent of the existing web app and controller.')
@minLength(2)
@maxLength(32)
param workerName string

param location string
param tags GovernanceTags

@description('Pinned deployment tenant. Not a credential.')
@minLength(36)
@maxLength(36)
param tenantId string

@description('Existing, dedicated user-assigned identity. The operator must grant image pull, required Fabric access and limited SQL permissions before deployment.')
param workerIdentityResourceId string

@description('Existing public ACR with admin/anonymous authentication disabled and ARM-audience authentication enabled. Pull uses only the selected managed identity.')
param registryResourceId string

@description('Verified Linux/amd64 image in the supplied registry, pinned as registry/repository@sha256:digest.')
param image string

@description('New public workload-profile environment name. Leave empty only when supplying an existing environment ID.')
param environmentName string = ''

@description('Optional verified public workload-profile environment with Consumption and azure-monitor logging to the supplied workspace. Neither it nor its diagnostic settings are redeployed.')
param existingEnvironmentResourceId string = ''

@description('Existing Log Analytics workspace resource ID. Azure Monitor diagnostic routing does not require workspace keys.')
param logAnalyticsWorkspaceResourceId string

@description('Optional reviewed exception tags for the NEW environment only. No exemption is enabled by default. These tags do not change Fabric network settings or existing Azure resources.')
param environmentExceptionTags object = {}

@description('Azure SQL hostname and catalog, without credentials or connection-string fields. The schema must already be bootstrapped by the deployment owner.')
param azureSqlServer string
param azureSqlDatabase string

@description('Nonsecret bootstrap metadata for an app-owned Eventstream destination. The worker must reconcile ownership and the active SQL manifest before consuming; this is not a monitored-target list.')
param connectorBootstrap ConnectorBootstrap

@description('API selection, not a permission grant. tenant_admin_preview explicitly enables admin workspaces/domains and preview Admin Items; source telemetry and remediation still require separate admission/probes.')
@allowed([
  'caller_visible'
  'tenant_admin_preview'
])
param inventoryMode string = 'caller_visible'

@description('One continuously running replica by default; two permits the explicit shared-state canary. maxReplicas is a ceiling, not an event-backlog autoscaler.')
@minValue(1)
@maxValue(2)
param minReplicas int = 1

resource workerIdentity 'Microsoft.ManagedIdentity/userAssignedIdentities@2023-01-31' existing = {
  name: last(split(workerIdentityResourceId, '/'))
  scope: resourceGroup(split(workerIdentityResourceId, '/')[2], split(workerIdentityResourceId, '/')[4])
}

resource registry 'Microsoft.ContainerRegistry/registries@2023-07-01' existing = {
  name: last(split(registryResourceId, '/'))
  scope: resourceGroup(split(registryResourceId, '/')[2], split(registryResourceId, '/')[4])
}

module newEnvironment './monitoring-environment.bicep' = if (empty(existingEnvironmentResourceId)) {
  name: '${workerName}-environment'
  params: {
    environmentName: environmentName
    location: location
    tags: tags
    environmentExceptionTags: environmentExceptionTags
    logAnalyticsWorkspaceResourceId: logAnalyticsWorkspaceResourceId
    diagnosticSettingName: '${workerName}-monitoring'
  }
}

var environmentId = empty(existingEnvironmentResourceId) ? newEnvironment!.outputs.environmentResourceId : existingEnvironmentResourceId

var workerEnvironment = {
  AZURE_SUBSCRIPTION_ID: subscription().subscriptionId
  AZURE_TENANT_ID: tenantId
  AZURE_CLIENT_ID: workerIdentity.properties.clientId
  MONITORING_IDENTITY_OBJECT_ID: workerIdentity.properties.principalId
  MONITORING_IDENTITY_RESOURCE_ID: workerIdentityResourceId
  MONITORING_MODE: 'live'
  MONITORING_INVENTORY_MODE: inventoryMode
  MONITORING_TENANT_ID: tenantId
  AZURE_SQL_SERVER: azureSqlServer
  AZURE_SQL_DATABASE: azureSqlDatabase
  MONITORING_CONNECTOR_ID: connectorBootstrap.connectorId
  MONITORING_EVENTSTREAM_WORKSPACE_ID: connectorBootstrap.workspaceId
  MONITORING_EVENTSTREAM_ID: connectorBootstrap.eventstreamId
  MONITORING_EVENTSTREAM_DESTINATION_ID: connectorBootstrap.destinationId
  MONITORING_EVENTSTREAM_NAMESPACE: connectorBootstrap.fullyQualifiedNamespace
  MONITORING_EVENTSTREAM_ENTITY: connectorBootstrap.eventHubName
  MONITORING_EVENTSTREAM_CONSUMER_GROUP: connectorBootstrap.consumerGroup
  PYTHONUNBUFFERED: '1'
  PYTHONDONTWRITEBYTECODE: '1'
}

resource worker 'Microsoft.App/containerApps@2025-01-01' = {
  name: workerName
  location: location
  tags: tags
  identity: {
    type: 'UserAssigned'
    userAssignedIdentities: {
      '${workerIdentityResourceId}': {}
    }
  }
  properties: {
    environmentId: environmentId
    workloadProfileName: 'Consumption'
    configuration: {
      activeRevisionsMode: 'Single'
      // Explicit null also removes ingress if a previous worker revision had it.
      ingress: null
      registries: [
        {
          server: registry.properties.loginServer
          identity: workerIdentityResourceId
        }
      ]
    }
    template: {
      terminationGracePeriodSeconds: 60
      containers: [
        {
          name: 'monitoring'
          image: image
          command: [
            'python'
            '-m'
            'triage.monitoring.worker'
          ]
          env: map(items(workerEnvironment), setting => {
            name: setting.key
            value: setting.value
          })
          resources: {
            cpu: json('0.5')
            memory: '1Gi'
          }
        }
      ]
      scale: {
        minReplicas: minReplicas
        maxReplicas: 2
        // SQL owns checkpoints; no HTTP scaler or credential-based hub scaler.
        rules: []
      }
    }
  }
}

output workerResourceId string = worker.id
output environmentResourceId string = environmentId
@description('Empty for a reused environment: its existing diagnostic route is verified by the operator script, not managed here.')
output diagnosticSettingResourceId string = empty(existingEnvironmentResourceId) ? newEnvironment!.outputs.diagnosticSettingResourceId : ''
output workerIdentityResourceId string = workerIdentity.id
output workerIdentityClientId string = workerIdentity.properties.clientId
output workerIdentityPrincipalId string = workerIdentity.properties.principalId
output registryLoginServer string = registry.properties.loginServer
output deployedImage string = image
output configuredWorkerEnvironment object = workerEnvironment
