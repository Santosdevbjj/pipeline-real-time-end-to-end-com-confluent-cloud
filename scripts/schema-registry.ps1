param(
  [Parameter(Mandatory=$true)][ValidateSet('check','register','compatibility')][string]$Action,
  [Parameter(Mandatory=$true)][string]$Subject,
  [Parameter(Mandatory=$true)][string]$SchemaPath,
  [string]$Compatibility = 'BACKWARD'
)
$ErrorActionPreference = 'Stop'
foreach ($name in @('SCHEMA_REGISTRY_URL','SCHEMA_REGISTRY_API_KEY','SCHEMA_REGISTRY_API_SECRET')) {
  if (-not [Environment]::GetEnvironmentVariable($name)) { throw "Missing environment variable: $name" }
}
$raw = Get-Content -Raw $SchemaPath
$schemaText = (ConvertFrom-Json $raw | ConvertTo-Json -Depth 100 -Compress)
$pair = "$($env:SCHEMA_REGISTRY_API_KEY):$($env:SCHEMA_REGISTRY_API_SECRET)"
$encoded = [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes($pair))
$headers = @{ Authorization = "Basic $encoded"; 'Content-Type' = 'application/vnd.schemaregistry.v1+json' }
$root = $env:SCHEMA_REGISTRY_URL.TrimEnd('/')
switch ($Action) {
  'check' {
    Invoke-RestMethod -Method Get -Uri "$root/subjects/$Subject/versions" -Headers $headers
  }
  'register' {
    $body = @{ schema = $schemaText; schemaType = 'AVRO' } | ConvertTo-Json -Compress
    Invoke-RestMethod -Method Post -Uri "$root/subjects/$Subject/versions" -Headers $headers -Body $body
  }
  'compatibility' {
    $body = @{ schema = $schemaText; schemaType = 'AVRO' } | ConvertTo-Json -Compress
    Invoke-RestMethod -Method Post -Uri "$root/compatibility/subjects/$Subject/versions/latest" -Headers $headers -Body $body
  }
}
