targetScope = 'resourceGroup'

import { GovernanceTags } from './monitoring-environment.bicep'

// Deployment preparation only: no SQL/admin, directory, network or application updates.
// A job starter can override its template and use this privileged identity.
// Keep that permission with the deployment operator, never runtime app roles.
// https://learn.microsoft.com/azure/container-apps/jobs
// https://learn.microsoft.com/azure/container-apps/managed-identity-image-pull

@minLength(3)
@maxLength(128)
param bootstrapIdentityName string

@minLength(2)
@maxLength(32)
param jobName string

@description('Existing Consumption environment in this resource group with public Azure SQL and registry connectivity. Not redeployed.')
param environmentName string

@description('Verified region of that existing environment. ARM requires location before runtime resource references are available.')
param location string

@description('Existing registry in this resource group. No keys, admin auth or registry/network changes.')
param registryName string

param tags GovernanceTags

@description('First deploy identity/AcrPull only. Fill its actual IDs into the reviewed image bundle before enabling the job.')
param deployJob bool = false

@description('Repository containing the parent-built current-source bootstrap image, NOT the old SDK/base application.')
param imageRepository string = 'state-sql-bootstrap'

@description('Immutable image SHA-256. Required even in identity-only preparation, where it is inert. Replace with the final approved bootstrap digest before deploying the job.')
@minLength(64)
@maxLength(64)
param imageDigest string

@description('Approved Azure SQL logical-server FQDN. TLS and Entra authentication remain required over the public TCP 1433 endpoint.')
param sqlServer string

param applicationDatabaseName string

@allowed([
  'application'
  'proof'
])
param targetKind string = 'proof'

@minLength(36)
@maxLength(36)
param tenantId string

@description('UUID of the approved bundle. Recovery identifies its replacement bundle here; the failed operation is bound inside the reviewed recovery request. A new start is not a new authorization.')
@minLength(36)
@maxLength(36)
param operationId string

@description('SHA-256 of the exact bundled manifest bytes, including source/batch hashes and metadata expectations.')
@minLength(64)
@maxLength(64)
param bundleSha256 string

@allowed([
  'preflight'
  'apply'
  'recover'
  'reconcile'
  'reconcile-recovery'
])
param mode string = 'preflight'

@description('Exact approved bundle file inside the execution. Application and proof bundles are distinct artifacts; do not relabel one as the other.')
@minLength(1)
param bundlePath string = '/opt/state-sql-bootstrap/bundle.json'

@description('Empty for preflight/reconcile. Apply and recover refuse unless this equals the reviewed bundle fingerprint.')
param approvedFingerprint string = ''

@description('Operator-staged original recovery JSON path for recover or reconcile-recovery. Fresh mutations require current evidence; historical read-only lookup preserves the original request.')
param recoveryPath string = ''

@description('SHA-256 of the exact recovery request. Its evidence window must be current and at most 15 minutes.')
@maxLength(64)
param recoverySha256 string = ''

@description('Separately approved recovery-request hash. Recover refuses unless it exactly matches recoverySha256; no schema batches execute in recover mode.')
@maxLength(64)
param approvedRecoverySha256 string = ''

@description('Optional immutable historical artifact directory, used only by reconcile-recovery. The current trusted reader validates archived bytes as data and never executes archived code.')
param artifactRoot string = ''

@minValue(60)
@maxValue(900)
param timeoutSeconds int = 600

resource environment 'Microsoft.App/managedEnvironments@2025-01-01' existing = {
  name: environmentName
}

resource registry 'Microsoft.ContainerRegistry/registries@2023-07-01' existing = {
  name: registryName
}

resource identity 'Microsoft.ManagedIdentity/userAssignedIdentities@2023-01-31' = {
  name: bootstrapIdentityName
  location: location
  tags: tags
}

var acrPullRole = subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '7f951dda-4ed3-4680-a7ca-43fe172d538d')

resource acrPull 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(registry.id, identity.id, acrPullRole)
  scope: registry
  properties: {
    principalId: identity.properties.principalId
    principalType: 'ServicePrincipal'
    roleDefinitionId: acrPullRole
  }
}

var image = '${registry.properties.loginServer}/${imageRepository}@sha256:${imageDigest}'
var databaseName = targetKind == 'proof' ? '${applicationDatabaseName}-proof' : applicationDatabaseName

resource job 'Microsoft.App/jobs@2025-01-01' = if (deployJob) {
  name: jobName
  location: location
  tags: tags
  identity: {
    type: 'UserAssigned'
    userAssignedIdentities: {
      '${identity.id}': {}
    }
  }
  properties: {
    environmentId: environment.id
    workloadProfileName: 'Consumption'
    configuration: {
      triggerType: 'Manual'
      replicaTimeout: timeoutSeconds
      replicaRetryLimit: 0
      manualTriggerConfig: {
        parallelism: 1
        replicaCompletionCount: 1
      }
      registries: [
        {
          server: registry.properties.loginServer
          identity: identity.id
        }
      ]
    }
    template: {
      containers: [
        {
          name: 'bootstrap'
          image: image
          command: [
            'python3'
            '-I'
            '-B'
          ]
          args: concat([
            '/opt/state-sql-bootstrap/scripts/bootstrap_azure_sql.py'
            '--bundle'
            bundlePath
            '--bundle-sha256'
            bundleSha256
            '--operation-id'
            operationId
            '--mode'
            mode
            '--approve-fingerprint'
            approvedFingerprint
          ], contains([
            'recover'
            'reconcile-recovery'
          ], mode) ? [
            '--recovery'
            recoveryPath
            '--recovery-sha256'
            recoverySha256
          ] : [], mode == 'recover' ? [
            '--approve-recovery-sha256'
            approvedRecoverySha256
          ] : [], mode == 'reconcile-recovery' && !empty(artifactRoot) ? [
            '--artifact-root'
            artifactRoot
          ] : [])
          env: [
            { name: 'AZURE_TENANT_ID', value: tenantId }
            { name: 'AZURE_CLIENT_ID', value: identity.properties.clientId }
            { name: 'BOOTSTRAP_IDENTITY_OBJECT_ID', value: identity.properties.principalId }
            { name: 'AZURE_SQL_SERVER', value: sqlServer }
            { name: 'AZURE_SQL_DATABASE', value: databaseName }
            { name: 'BOOTSTRAP_APPLICATION_DATABASE', value: applicationDatabaseName }
            { name: 'BOOTSTRAP_TARGET_KIND', value: targetKind }
          ]
          resources: {
            cpu: json('0.25')
            memory: '0.5Gi'
          }
        }
      ]
    }
  }
  dependsOn: [
    acrPull
  ]
}

output bootstrapIdentityResourceId string = identity.id
output bootstrapIdentityClientId string = identity.properties.clientId
output bootstrapIdentityObjectId string = identity.properties.principalId
output acrPullAssignmentId string = acrPull.id
output jobResourceId string = deployJob ? job.id : ''
output jobDeployed bool = deployJob
output sqlExecuted bool = false
output sqlAdministratorChanged bool = false
output runtimeAcceptanceRequired bool = true
