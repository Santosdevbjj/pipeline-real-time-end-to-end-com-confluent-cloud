param([switch]$Apply)
$ErrorActionPreference = 'Stop'
$terraformDir = Join-Path $PSScriptRoot '../infra/terraform'
Push-Location $terraformDir
try {
  terraform init
  terraform validate
  terraform plan -out payments.tfplan
  if ($Apply) {
    Write-Warning 'Applying creates Confluent Cloud resources that may incur charges.'
    terraform apply payments.tfplan
  } else {
    Write-Host 'Plan saved. Review it, then run .\scripts\setup.ps1 -Apply to create resources.'
  }
} finally { Pop-Location }
