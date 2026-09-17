targetScope = 'resourceGroup'

import { GovernanceTags } from './monitoring-environment.bicep'

// Build the narrow Dockerfile.monitoring-transport-probe context separately.
// This manual job never starts a worker, creates Fabric items or reads keys.
// https://learn.microsoft.com/azure/container-apps/jobs

@sealed()
type ProbeEndpoint = {
  type: 'CustomEndpoint'
  fullyQualifiedNamespace: string
  eventHubName: string
  consumerGroupName: string
  workspaceId: string
  eventstreamId: string
  destinationId: string
}

@minLength(2)
@maxLength(32)
param jobName string
param location string
param tags GovernanceTags
param tenantId string

@description('Verified dedicated monitoring environment, shared with the bootstrap job and later worker.')
param environmentResourceId string

@description('Existing explicitly selected UAMI with ACR pull and owned Fabric workspace access. No grants are created.')
param workerIdentityResourceId string
param registryResourceId string

@description('Repository of the separately built public transport-probe image in the supplied ACR.')
param imageRepository string

@description('Actual verified image manifest digest, without the sha256: prefix. Never a tag or placeholder.')
@minLength(64)
@maxLength(64)
param imageDigest string

@description('isolated uses the dedicated probe image entrypoint. bundled-public-helper runs the compiled, public-only receive bundle on an existing Python diagnostic image; no post-staging worker module is assumed.')
@allowed([
  'isolated'
  'bundled-public-helper'
])
param receiverMode string = 'isolated'

@description('Explicit bundled-image bootstrap only: install pinned aiohttp 3.14.3 into a temporary directory from trusted PyPI, with no retries. False requires aiohttp already in the image. This does not install worker code or change the image.')
param installPinnedAsyncTransport bool = false

@description('Only approved Entra-tab metadata; no accessKeys, SAS or connection API response.')
param endpointMetadata ProbeEndpoint
param sourceWorkspaceId string
param sourceItemId string

@description('Optional owned model UUID for independent read-only source/admin readiness projections. Empty disables all extra REST probes; preview API gaps never fail transport acceptance.')
param readinessModelId string = ''

@minValue(120)
@maxValue(240)
param probeSeconds int = 120

resource identity 'Microsoft.ManagedIdentity/userAssignedIdentities@2023-01-31' existing = {
  name: last(split(workerIdentityResourceId, '/'))
  scope: resourceGroup(split(workerIdentityResourceId, '/')[2], split(workerIdentityResourceId, '/')[4])
}

resource registry 'Microsoft.ContainerRegistry/registries@2023-07-01' existing = {
  name: last(split(registryResourceId, '/'))
  scope: resourceGroup(split(registryResourceId, '/')[2], split(registryResourceId, '/')[4])
}

var input = union({
  endpoint: endpointMetadata
  sourceWorkspaceId: sourceWorkspaceId
  sourceItemId: sourceItemId
  seconds: probeSeconds
}, empty(readinessModelId) ? {} : { readinessModelId: readinessModelId })
var image = '${registry.properties.loginServer}/${imageRepository}@sha256:${imageDigest}'

resource probe 'Microsoft.App/jobs@2025-01-01' = {
  name: jobName
  location: location
  tags: tags
  identity: {
    type: 'UserAssigned'
    userAssignedIdentities: {
      '${workerIdentityResourceId}': {}
    }
  }
  properties: {
    environmentId: environmentResourceId
    workloadProfileName: 'Consumption'
    configuration: {
      triggerType: 'Manual'
      replicaTimeout: probeSeconds + (receiverMode == 'bundled-public-helper' && installPinnedAsyncTransport ? 180 : 60)
      replicaRetryLimit: 0
      manualTriggerConfig: {
        parallelism: 1
        replicaCompletionCount: 1
      }
      registries: [
        {
          server: registry.properties.loginServer
          identity: workerIdentityResourceId
        }
      ]
    }
    template: {
      containers: [
        {
          name: 'transport-probe'
          image: image
          command: receiverMode == 'isolated' ? [
            'python'
            '-m'
            'scripts.monitoring_transport_probe'
          ] : [
            'python'
            '-c'
            loadTextContent('../scripts/transport_probe_image_entry.py')
          ]
          env: [
            { name: 'AZURE_TENANT_ID', value: tenantId }
            { name: 'AZURE_SUBSCRIPTION_ID', value: subscription().subscriptionId }
            { name: 'AZURE_CLIENT_ID', value: identity.properties.clientId }
            { name: 'MONITORING_IDENTITY_OBJECT_ID', value: identity.properties.principalId }
            { name: 'MONITORING_IDENTITY_RESOURCE_ID', value: workerIdentityResourceId }
            { name: 'MONITORING_TRANSPORT_PROBE_INPUT', value: string(input) }
            { name: 'PYTHONUNBUFFERED', value: '1' }
            { name: 'MONITORING_PROBE_INSTALL_AIOHTTP', value: installPinnedAsyncTransport ? 'true' : 'false' }
            { name: 'PUBLIC_TRANSPORT_HELPER', value: loadFileAsBase64('../scripts/hybrid_platform_probe.py') }
            { name: 'PUBLIC_READINESS_HELPER', value: loadFileAsBase64('../scripts/monitoring_readiness_probe.py') }
            { name: 'PUBLIC_TRANSPORT_RUNNER', value: loadFileAsBase64('../scripts/monitoring_transport_probe.py') }
          ]
          resources: {
            cpu: json('0.5')
            memory: '1Gi'
          }
        }
      ]
    }
  }
}

output jobResourceId string = probe.id
output environmentResourceId string = environmentResourceId
output image string = image
output executionRequired bool = true
output normalWorkerReady bool = false
output eventConsumptionVerified bool = false
output receiverMode string = receiverMode
