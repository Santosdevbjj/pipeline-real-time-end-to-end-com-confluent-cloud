# Schema Registry

O connector CDC registra automaticamente os schemas Avro Debezium (envelope `before`/`after`/`op`) nos subjects vinculados aos tópicos, normalmente `<topic>-key` e `<topic>-value`. Não registre o schema normalizado `payments-value.avsc` sobre esse subject: ele serve como exemplo independente para exercício de compatibilidade. Para o alerta, a tabela Flink cria o tópico e seus schemas; valide que o subject gerado corresponda às colunas do sink.

Para exercitar compatibilidade sem tocar num subject ativo, registre primeiro `payments-value.avsc` num subject de laboratório, por exemplo `payments.contract.payment-value`, e rode `scripts/schema-registry.ps1 -Action compatibility` com o mesmo arquivo (esperado `is_compatible: true`). Depois rode com `payments-value-breaking.avsc` (esperado `false`, pois remove `transaction_id`). Só então use `-Action register` para registrar versões de contratos aprovadas. Credenciais são lidas de variáveis de ambiente; veja `.env.example`.

`payments-value.avsc` é um contrato normalizado de exercício e não o envelope emitido pelo connector CDC. `amount_minor` representa centavos da menor unidade monetária para moedas de duas casas; um sistema multimoeda deve carregar escala ISO 4217 em dimensão explícita e não assumir duas casas para todas as moedas.
