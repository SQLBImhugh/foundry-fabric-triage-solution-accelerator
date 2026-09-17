targetScope = 'resourceGroup'

// Optional creation-only helper after the operator has listed BOTH scopes. Capability
// hosts are immutable and limited to one per scope; never guess a "default" host.
// https://learn.microsoft.com/azure/foundry/agents/concepts/capability-hosts
// https://learn.microsoft.com/rest/api/microsoftfoundry/accountmanagement/project-capability-hosts/create-or-update

@minLength(2)
@maxLength(54)
param accountName string

@minLength(2)
@maxLength(64)
param projectName string

@description('True only after a fresh successful list proved the NEW account has no capability host. Never delete an existing host to make this true.')
param createAccountCapabilityHost bool

@description('True only after a fresh successful list proved the NEW project has no capability host.')
param createProjectCapabilityHost bool

@description('Explicit operator-selected create name, not an inferred platform default.')
@minLength(1)
@maxLength(64)
param accountCapabilityHostName string

@description('Explicit operator-selected create name. Omitted BYO connection lists select the platform-managed basic resource contract.')
@minLength(1)
@maxLength(64)
param projectCapabilityHostName string

resource account 'Microsoft.CognitiveServices/accounts@2025-06-01' existing = {
  name: accountName
}

resource project 'Microsoft.CognitiveServices/accounts/projects@2025-06-01' existing = {
  parent: account
  name: projectName
}

resource accountCapabilityHost 'Microsoft.CognitiveServices/accounts/capabilityHosts@2025-06-01' = if (createAccountCapabilityHost) {
  parent: account
  name: accountCapabilityHostName
  properties: {
    capabilityHostKind: 'Agents'
  }
}

// ProjectCapabilityHost has no writable capabilityHostKind in the current REST
// schema. Do not copy the account-only field from older sample project modules.
resource projectCapabilityHost 'Microsoft.CognitiveServices/accounts/projects/capabilityHosts@2025-06-01' = if (createProjectCapabilityHost) {
  parent: project
  name: projectCapabilityHostName
  properties: {}
  dependsOn: [
    accountCapabilityHost
  ]
}

output accountCapabilityHostResourceId string = createAccountCapabilityHost ? accountCapabilityHost.id : ''
output projectCapabilityHostResourceId string = createProjectCapabilityHost ? projectCapabilityHost.id : ''
output hostedAgentAcceptanceRequired bool = true
