# Operação, segurança e runbook

## Sinais

| Pilar | Sinal | Ação quando fora do esperado |
|---|---|---|
| Observabilidade | connector RUNNING, erros de task, bytes recebidos, lag do consumer e estado/idade do statement | Verificar conectividade, slot WAL, schema incompatível, quotas e checkpoint; preservar offsets antes de reiniciar |
| Segurança | ACL por service account, acesso ao banco, chaves ativas e TLS | Revogar chave exposta, rotacionar, revisar audit log e reduzir grants |
| Custos | consumo do cluster, connector, Flink compute/storage e billing por período | Pausar statements, dimensionar throughput/retention e desligar recursos ociosos |
| Confiabilidade | checkpoint concluído, restart count, watermark e DLQ/erros | Investigar backpressure e late events; validar recuperação e idempotência |

## Resposta a falhas

1. **CDC parado:** confirme reachability/TLS, credencial, publicação/slot e espaço WAL. Não descarte slot sem avaliar perda/re-snapshot.
2. **Schema incompatível:** pare a publicação da versão, compare schema e compatibility result, faça evolução aditiva e recupere connector após validação.
3. **Flink FAILED/backpressure:** capture statement diagnostics, checkpoint e lag; corrija causa e reinicie de estado consistente. Confira duplicatas no consumidor.
4. **Alerta em excesso:** trate como sinal de triagem, não como prova de fraude; revise janela e limiar com dados rotulados.

## Teardown

Pare/encerre os statements Flink, pause e delete o connector, remova credenciais específicas do lab, depois rode `terraform destroy` a partir de `infra/terraform/`. Confirme no console/API que cluster, ACLs, service accounts, connector e statements foram removidos. Retenha billing/evidências sanitizadas e destrua o banco de origem separadamente se ele foi criado para o laboratório.
