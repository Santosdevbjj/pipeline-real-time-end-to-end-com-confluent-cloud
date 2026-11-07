# Captura de evidências reais

Guarde capturas sanitizadas em `captures/` ou anexe texto sem chaves, endpoints privados ou dados pessoais. Cada evidência deve incluir data, região, comando/tela e identificador não sensível do recurso.

- [ ] Fundação: ambiente, cluster e ACLs (identidades producer/consumer).
- [ ] Contrato: resultado positivo para novo campo opcional com default e negativo para remoção de campo obrigatório.
- [ ] Ingestão: evento `c`, evento `u` com `before`/`after`, evento `d` e tombstone; registrar lag/slot.
- [ ] Flink: estado RUNNING do statement, alerta do tópico e três transações que o causaram.
- [ ] Operação: bytes/throughput, consumer lag, erros, controle de acesso e confiabilidade (checkpoint/restart).
- [ ] Custos: valor real faturado, período, moeda, serviços incluídos e créditos/descontos.
- [ ] Teardown: connector removido, statement parado, `terraform destroy` e listagem final sem recursos.

Não marque itens concluídos antes de capturar saídas da sua conta. Redija segredos antes de salvar.
