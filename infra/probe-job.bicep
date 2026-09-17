targetScope = 'resourceGroup'

import { GovernanceTags } from './monitoring-environment.bicep'

// A manual job in the NEW monitoring environment, not an existing application.
// Only public helper code is embedded; inputs are a bounded nonsecret canary spec.
// Job start permission can override execution templates and use this UAMI.
// Grant it only to the trusted deployment operator; no roles are granted here.
// https://learn.microsoft.com/azure/container-apps/jobs
// https://learn.microsoft.com/cli/azure/run-azure-cli-docker

@minLength(2)
@maxLength(32)
param jobName string

param location string
param tags GovernanceTags

@description('Verified resource ID output from monitoring-environment.bicep. Use the same dedicated environment for the later worker.')
param environmentResourceId string

@description('Existing dedicated UAMI. No identity or grants are created by this template.')
param workerIdentityResourceId string

param tenantId string

@description('Public Azure Linux 3.0 Azure CLI image index digest, verified with MCR on 2026-09-15; includes linux/amd64. Refresh through an approved image review.')
@minLength(64)
@maxLength(64)
param cliImageDigest string = 'e3768dde8142efa45d8f356a317aaac77abd7da15ba3719b0a150e9453f251db'

@description('Explicitly journalled canary intent UUID. Serialize job starts for this intent; client-request-id is not server idempotency.')
param intentId string
param workspaceId string
param pipelineId string

@description('Inspect is read-only and is the safe default. Create submits at most one Eventstream create POST; resume polls an existing creation operation.')
@allowed([
  'inspect'
  'create'
  'resume'
])
param action string = 'inspect'

@description('Creation LRO UUID, required only for resume. Never an endpoint URL or key.')
param operationId string = ''

@minValue(30)
@maxValue(480)
param timeoutSeconds int = 240

resource identity 'Microsoft.ManagedIdentity/userAssignedIdentities@2023-01-31' existing = {
  name: last(split(workerIdentityResourceId, '/'))
  scope: resourceGroup(split(workerIdentityResourceId, '/')[2], split(workerIdentityResourceId, '/')[4])
}

var input = union({
  action: action
  intentId: intentId
  workspaceId: workspaceId
  pipelineId: pipelineId
  timeoutSeconds: timeoutSeconds
}, empty(operationId) ? {} : { operationId: operationId })

var bootstrap = '''
import base64, os, sys, types
package = types.ModuleType("scripts")
package.__path__ = []
sys.modules["scripts"] = package
helper = types.ModuleType("scripts.hybrid_platform_probe")
sys.modules[helper.__name__] = helper
exec(compile(base64.b64decode(os.environ["PUBLIC_PROBE_HELPER"]), "<public-probe-helper>", "exec"), helper.__dict__)
exec(compile(base64.b64decode(os.environ["PUBLIC_CANARY_RUNNER"]), "<public-canary-runner>", "exec"), globals())
'''

resource canary 'Microsoft.App/jobs@2025-01-01' = {
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
      replicaTimeout: 600
      replicaRetryLimit: 0
      manualTriggerConfig: {
        parallelism: 1
        replicaCompletionCount: 1
      }
    }
    template: {
      containers: [
        {
          name: 'canary'
          image: 'mcr.microsoft.com/azure-cli@sha256:${cliImageDigest}'
          command: [
            'python3'
          ]
          args: [
            '-c'
            bootstrap
          ]
          env: [
            { name: 'AZURE_TENANT_ID', value: tenantId }
            { name: 'AZURE_SUBSCRIPTION_ID', value: subscription().subscriptionId }
            { name: 'AZURE_CLIENT_ID', value: identity.properties.clientId }
            { name: 'MONITORING_IDENTITY_OBJECT_ID', value: identity.properties.principalId }
            { name: 'MONITORING_IDENTITY_RESOURCE_ID', value: workerIdentityResourceId }
            { name: 'MONITORING_CANARY_INPUT', value: string(input) }
            { name: 'PUBLIC_PROBE_HELPER', value: loadFileAsBase64('../scripts/hybrid_platform_probe.py') }
            { name: 'PUBLIC_CANARY_RUNNER', value: loadFileAsBase64('../scripts/monitoring_eventstream_canary.py') }
            { name: 'PYTHONUNBUFFERED', value: '1' }
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

output jobResourceId string = canary.id
output environmentResourceId string = environmentResourceId
output image string = 'mcr.microsoft.com/azure-cli@sha256:${cliImageDigest}'
output executionRequired bool = true
output workerReady bool = false
output eventTransportVerified bool = false
