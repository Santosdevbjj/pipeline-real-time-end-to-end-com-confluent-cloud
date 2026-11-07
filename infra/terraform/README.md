# Infraestrutura Confluent Cloud

Este módulo provisiona ambiente, cluster Basic, service accounts e ACLs básicas. O uso do provider pode criar recursos cobrados. Leia o plano e confira região, limites e preço na conta antes de aplicar.

```powershell
$env:TF_VAR_confluent_cloud_api_key = '...'
$env:TF_VAR_confluent_cloud_api_secret = '...'
Copy-Item terraform.tfvars.example terraform.tfvars
terraform init
terraform plan
terraform apply
```

Terraform state contém metadados sensíveis, inclusive os secrets das Kafka API keys, e deve ficar em backend remoto criptografado e protegido por RBAC. Não comite state nem credenciais. Outputs de secrets são marcados sensitive, mas continuam gravados no state. Injete-os no workspace do connector/consumer por um secret manager. Schema Registry e Flink compute/API keys podem exigir habilitação e configuração adicional na região/ambiente; confirme disponibilidade, permissões RBAC e custos.

```powershell
terraform destroy
```

O destroy remove recursos declarados no state. Remova antes connector e statements criados fora deste Terraform e confira o console ao final.
