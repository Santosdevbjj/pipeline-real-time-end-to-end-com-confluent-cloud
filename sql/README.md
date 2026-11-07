# Execução dos statements Flink

1. Crie/valide os tópicos e schemas gerados pelo connector. Selecione o catalog `payments-lab` e database `payments-basic` no workspace e rode `SHOW TABLES`.
2. Execute `00_sources.sql`; confirme que os tópicos CDC inferidos apresentam o envelope Debezium. O Flink Cloud usa `avro-debezium-registry` para envelopes Avro e `retract` para capturar inserts, updates e deletes. Ajuste os nomes qualificados no SQL aos nomes exibidos pelo seu ambiente.
3. Execute `10_alert_sink.sql` e acompanhe o estado do statement e o tópico `payments.fraud.alerts` (a tabela cria o tópico de saída e os schemas associados).
4. Insira três pagamentos autorizados para o mesmo cartão com timestamps dentro de 60 segundos. Confira o alerta, `event_count >= 3`, conta enriquecida e `rule_id`.

O SQL usa HOP para produzir janelas deslizantes de um segundo com tamanho de 60 segundos. A tabela de origem tem chave primária declarada para que UPDATE/DELETE CDC atualize o estado da agregação. `transaction_ts` é timestamp de evento e watermark tolera cinco segundos; atraso maior pode ser descartado conforme política do runtime. Ajuste retenção, checkpoint e tratamento de late events antes de produção.

Como a saída é upsert por chave `alert_id`, o mesmo cartão pode atualizar alertas de janelas adjacentes; o consumidor deve ser idempotente. Se precisar de um único alerta por episódio, implemente deduplicação/supressão explícita e valide a semântica de janela com testes de dados controlados.
