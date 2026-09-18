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

@description('Optional application-owned Insights resource. Its log alert detects zero completed heartbeats even when the platform metric emits no samples.')
param applicationInsightsResourceId string = ''

@description('Region of the selected Application Insights resource.')
param applicationInsightsLocation string = heartbeatWorkflowLocation

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

resource absentTelemetry 'Microsoft.Insights/scheduledQueryRules@2023-12-01' = if (!empty(applicationInsightsResourceId)) {
  name: '${namePrefix}-heartbeat-telemetry-missing'
  location: applicationInsightsLocation
  kind: 'LogAlert'
  tags: tags
  properties: {
    displayName: 'Controller heartbeat telemetry missing'
    description: 'No completed application heartbeat was ingested for 15 minutes. Check the timer, controller and telemetry; this does not authorize retrying a workload effect.'
    severity: 2
    enabled: true
    autoMitigate: true
    evaluationFrequency: 'PT1M'
    windowSize: 'PT15M'
    scopes: [
      applicationInsightsResourceId
    ]
    skipQueryValidation: false
    criteria: {
      allOf: [
        {
          // summarize returns one zero-count row when no heartbeat exists.
          query: '''
traces
| where timestamp > ago(15m)
| where message startswith "heartbeat_finished status=completed "
| summarize completed_heartbeats=count()
'''
          metricMeasureColumn: 'completed_heartbeats'
          timeAggregation: 'Total'
          operator: 'LessThan'
          threshold: 1
          failingPeriods: {
            numberOfEvaluationPeriods: 1
            minFailingPeriodsToAlert: 1
          }
        }
      ]
    }
    actions: {
      actionGroups: actionGroupResourceIds
    }
  }
}

output alertResourceIds array = [for (check, index) in checks: alerts[index].id]
output telemetryAbsenceAlertResourceId string = !empty(applicationInsightsResourceId) ? absentTelemetry!.id : ''
output externalNotificationsConfigured bool = !empty(actionGroupResourceIds)
