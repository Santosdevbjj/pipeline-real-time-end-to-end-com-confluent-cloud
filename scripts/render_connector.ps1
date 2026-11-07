param(
  [Parameter(Mandatory=$true)][string]$Template = "$PSScriptRoot/../connectors/postgres-cdc.json",
  [Parameter(Mandatory=$true)][string]$Output = "$PSScriptRoot/../connectors/payments-cdc-rendered.json"
)
$ErrorActionPreference = 'Stop'
$required = @('KAFKA_API_KEY','KAFKA_API_SECRET','POSTGRES_HOST','POSTGRES_USER','POSTGRES_PASSWORD')
foreach ($name in $required) {
  if (-not [Environment]::GetEnvironmentVariable($name)) { throw "Missing environment variable: $name" }
}
$raw = Get-Content -Raw $Template
$raw = $raw.Replace('${file:/secrets:KAFKA_API_KEY}', $env:KAFKA_API_KEY)
$raw = $raw.Replace('${file:/secrets:KAFKA_API_SECRET}', $env:KAFKA_API_SECRET)
$raw = $raw.Replace('${file:/secrets:POSTGRES_HOST}', $env:POSTGRES_HOST)
$raw = $raw.Replace('${file:/secrets:POSTGRES_USER}', $env:POSTGRES_USER)
$raw = $raw.Replace('${file:/secrets:POSTGRES_PASSWORD}', $env:POSTGRES_PASSWORD)
$raw | Set-Content -NoNewline -Encoding utf8 $Output
Write-Warning 'Rendered file contains secrets. It is gitignored; protect it and delete it after connector creation.'
