# Referências técnicas

Documentação oficial consultada para o desenho (confira a documentação disponível no momento em que executar, pois versões e propriedades mudam):

- [PostgreSQL CDC Source V2 (Debezium)](https://docs.confluent.io/cloud/current/connectors/cc-postgresql-cdc-source-v2-debezium/cc-postgresql-cdc-source-v2-debezium.html) — formatos Avro/JSON, nomes de tópico, slot/publication, tombstones, SSL e opções do connector.
- [Debezium CDC format no Flink Cloud](https://docs.confluent.io/cloud/current/flink/reference/sql-examples.html#debezium-cdc-format-in-schema-registry) — `avro-debezium-registry`, detecção do envelope e changelog.
- [CREATE TABLE no Confluent Cloud for Apache Flink](https://docs.confluent.io/cloud/current/flink/reference/statements/create-table.html) — tópicos/tabelas e formatos suportados pelo conector `confluent`.
- [Terraform provider Confluent](https://docs.confluent.io/cloud/current/clusters/terraform-provider.html) — recursos e credenciais do provider.
- [Service accounts para Confluent Cloud connectors](https://docs.confluent.io/cloud/current/connectors/service-account.html) — ACLs e criação de Kafka API keys.

O connector tem propriedades específicas da versão e da plataforma PostgreSQL. O JSON deste projeto fornece parâmetros comuns; use a referência da sua versão e valide networking, IAM/ACLs e permissões no cluster antes de criar.
