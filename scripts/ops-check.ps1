param(
  [Parameter(Mandatory=$false)][string]$ClusterId = $env:CONFLUENT_CLUSTER_ID
)
$ErrorActionPreference = 'Stop'
if (-not $ClusterId) { throw 'Informe -ClusterId ou CONFLUENT_CLUSTER_ID.' }
Write-Host 'Checklist: execute e salve saídas sanitizadas para evidências.'
confluent kafka cluster describe $ClusterId
confluent kafka acl list --cluster $ClusterId
confluent kafka topic list --cluster $ClusterId
Write-Host 'Consulte connector status, Flink statement status, Metrics API e Billing no console Confluent.'
