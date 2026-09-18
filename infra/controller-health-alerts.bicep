targetScope = 'resourceGroup'

import { GovernanceTags } from './monitoring-environment.bicep'

@description('Resource ID of the single deployed heartbeat Logic App. Its response validation must reject failed controller responses.')
param heartbeatWorkflowResourceId string

@description('Region of that existing workflow.')
param heartbeatWorkflowLocation string

@description('Distinct prefix for the two controller-health alerts.')
param namePrefix string

param tags GovernanceTags

@description('Optional approved notification routes. Empty means Azure Monitor portal alerts only; no email or webhook is inferred.')
param actionGroupResourceIds string[] = []

var checks = [
  {
    suffix: 'heartbeat-missing'
    metric: 'RunsSucceeded'
    operator: 'LessThan'
    threshold: 1
    window: 'PT15M'
    description: 'No heartbeat completed successfully within 15 minutes. Inspect the scheduler and controller; do not retry an uncertain workload effect.'
  }
  {
    suffix: 'heartbeat-failed'
    metric: 'RunsFailed'
    operator: 'GreaterThan'
    threshold: 0
    window: 'PT5M'
    description: 'A heartbeat invocation failed. HTTP success alone is not controller success; the workflow validates the completed response.'
  }
]

resource alerts 'Microsoft.Insights/metricAlerts@2018-03-01' = [for check in checks: {
  name: '${namePrefix}-${check.suffix}'
  location: 'global'
  tags: tags
  properties: {
    description: check.description
    severity: 2
    enabled: true
    autoMitigate: true
    evaluationFrequency: 'PT1M'
    windowSize: check.window
    scopes: [
      heartbeatWorkflowResourceId
    ]
    targetResourceType: 'Microsoft.Logic/workflows'
    targetResourceRegion: heartbeatWorkflowLocation
    criteria: {
      'odata.type': 'Microsoft.Azure.Monitor.SingleResourceMultipleMetricCriteria'
      allOf: [
        {
          name: check.suffix
          criterionType: 'StaticThresholdCriterion'
          metricNamespace: 'Microsoft.Logic/workflows'
          metricName: check.metric
          operator: check.operator
          threshold: check.threshold
          timeAggregation: 'Total'
        }
      ]
    }
    actions: [for id in actionGroupResourceIds: {
      actionGroupId: id
    }]
  }
}]

output alertResourceIds array = [for (check, index) in checks: alerts[index].id]
output externalNotificationsConfigured bool = !empty(actionGroupResourceIds)
