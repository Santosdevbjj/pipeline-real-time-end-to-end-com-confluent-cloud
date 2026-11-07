## Bootcamp IBM Confluent - Dados em tempo real para agentes de IA.

<img width="120" height="120" alt="1000135685" src="https://github.com/user-attachments/assets/7696b416-285d-4c3f-8155-f4e92c7019f0" />

---

## DESCRIÇÃO:

Chegou a hora de montar um pipeline de pagamentos que roda em tempo real no Confluent Cloud. 

> Você leva os dados de um Postgres para o streaming por CDC, processa com Flink SQL para enriquecer as transações e marcar as suspeitas, e entrega o resultado para quem vai consumir. 

> São cinco camadas, da fundação do ambiente até a operação com métricas e custo, e os dados sintéticos já vêm prontos para você começar. 

> No fim, você publica um repositório no GitHub com o README documentando camada por camada, com a evidência de que cada uma rodou.


---

# Pipeline de pagamentos em tempo real

Pipeline de referência para capturar transações PostgreSQL por CDC, validar contratos Avro, enriquecer o fluxo no Confluent Cloud for Apache Flink e emitir alertas de risco. O foco é demonstrar o caminho e as decisões de engenharia com passos reprodutíveis; evidências de execução e custo só podem ser preenchidas depois de executar na sua própria conta.

> **Estado da entrega:** implementação e roteiro prontos. Os blocos `evidence/` contêm instruções e modelos; não afirmam que recursos cloud foram provisionados. Não publique credenciais nem substitua evidências por saídas inventadas.

## Problema de negócio

Pagamentos suspeitos precisam ser detectados enquanto ainda podem ser revisados. Processamentos em lote aumentam a latência entre a atividade do cartão e a sinalização. Este projeto entrega um evento de alerta quando um cartão apresenta três ou mais transações em uma janela de 60 segundos, enriquecendo-o com dados da conta.

## Contexto e baseline

O domínio usa dados sintéticos determinísticos, sem dados pessoais reais. A baseline operacional é a ausência de detecção contínua: eventos são avaliados individualmente e não existe alerta por frequência em janela. Não há alegação de redução de fraude ou de latência medida antes da execução; essas métricas devem ser coletadas no ambiente do operador.

## Premissas

- PostgreSQL é a fonte da verdade; `transactions` tem chave primária estável e `updated_at`.
- Debezium PostgreSQL publica eventos CDC com envelope Debezium e tombstones Kafka para exclusões.
- O conector precisa alcançar o PostgreSQL pela rede. Use um banco gerenciado com TLS e publicação lógica habilitada; não exponha uma instância local à internet.
- `amount` é armazenado em centavos inteiros e convertido para decimal na camada analítica.
- A regra é heurística de triagem, não decisão automática de fraude. A janela é processada por cartão.
- Custos e disponibilidade dependem de região, tiers, retenção e tempo de execução na conta Confluent.

## Planejamento da solução

| Camada | Componente | Responsabilidade |
|---|---|---|
| Fundação | Confluent Cloud, ambiente/cluster, service accounts e ACLs | Identidades de escrita/leitura com privilégio mínimo |
| Contrato | Schema Registry + Avro | Evolução compatível e campos sensíveis minimizados |
| Ingestão | PostgreSQL + Debezium CDC | Capturar insert/update/delete sem polling |
| Processamento | Flink SQL | Desembrulhar CDC, enriquecer e detectar frequência |
| Operação | métricas, ACL audit, billing e runbook | Observar, proteger, estimar custo e encerrar recursos |

O roteiro foi desenhado para Confluent Cloud. A forma recomendada de prova local é validar o PostgreSQL/seed antes de conectar um endpoint PostgreSQL acessível pelo conector cloud. O conector gerenciado não consegue acessar `localhost` do computador do operador.

## Limpeza e qualidade dos dados

O seed usa dados sintéticos com IDs fixos e timestamps relativos ao instante de carga. As tabelas aplicam PK, FK, `NOT NULL`, moeda ISO e valores positivos. No stream, mensagens inválidas ou sem campos necessários são excluídas das regras e contabilizadas pelo consumidor/monitoramento; não são reinterpretadas silenciosamente. O CDC mantém before/after para auditoria e o pipeline trata `op=d` e tombstones de acordo com a semântica de deleção.

## EDA / validação de hipóteses

Antes de ativar alertas, consulte volume por conta, distribuição de valor e transações por cartão. As consultas estão em [`sql/validation.sql`](sql/validation.sql). A hipótese testada pelo streaming é: “três pagamentos do mesmo cartão dentro de 60 segundos merecem revisão”. Ela não demonstra causalidade nem estima precisão/recall; isso exigiria rótulos de fraude e avaliação offline.

## Contrato de dados

Os esquemas Avro de exemplo estão em [`schemas/`](schemas/). O connector gera o envelope CDC registrado no Registry; o schema de pagamento normalizado é usado apenas no exercício isolado de compatibilidade. `card_id` é sintético/tokenizado. Veja [`schemas/README.md`](schemas/README.md) e os comandos em `scripts/`.

### Framework de 11 etapas, adaptado ao streaming

1. Problema de negócio; 2. baseline sem alerta contínuo; 3. planejamento da solução e componentes; 4. qualidade/limpeza e semântica CDC; 5. EDA e hipóteses verificadas em SQL; 6. preparação/contratos e tipos; 7. regra de processamento Flink no lugar de treinamento de modelo; 8. performance técnica medida por latência/lag e erros; 9. performance de negócio avaliada por revisão, falsos positivos e falsos negativos; 10. operação; 11. storytelling com diagrama, eventos e recomendações. As etapas 8 e 9 não têm números até serem medidas em execução.

## Implementação: camadas do pipeline

### 1. Fundação

Use [`infra/terraform/`](infra/terraform/) para criar ambiente, cluster, service accounts e ACLs segregadas. Credenciais são exportadas apenas para o shell local e não ficam no Terraform state versionado. O backend remoto do state e sua criptografia devem ser configurados pelo operador em produção. Revise `terraform plan` antes de aplicar: recursos Confluent Cloud podem gerar cobrança.

### 2. Contrato

Registre os schemas Avro no Schema Registry do ambiente. Rode o teste de compatibilidade antes de publicar uma nova versão. Há exemplos aditivo compatível e remoção incompatível; use os comandos documentados no diretório `schemas/`.

### 3. Ingestão (CDC)

Suba um PostgreSQL gerenciado com logical replication (`wal_level=logical`) e crie usuário restrito `cdc_user` com permissões de replicação/leitura nas tabelas necessárias. Configure TLS, publicação e slot conforme a plataforma. O conector `PostgresCdcSourceV2` está parametrizado em [`connectors/postgres-cdc.json`](connectors/postgres-cdc.json); `scripts/render_connector.ps1` preenche variáveis sem gravar segredo no arquivo. Confirme os nomes de campos de conexão e opções na versão do conector selecionada no seu cluster antes de criar.

O seed está em [`postgres/init/`](postgres/init/) e é reproduzível. Insira, atualize e exclua uma linha de teste e confirme `op=c`, `op=u`, `op=d` no tópico de origem. Debezium representa delete como evento `d` seguido por tombstone; ambos precisam ser reconhecidos pelo consumidor.

### 4. Processamento (Flink SQL)

Os DDLs e statements estão em [`sql/`](sql/). A consulta de enriquecimento associa pagamentos a contas pelo identificador e a regra `MATCH_RECOGNIZE` emite um alerta para 3+ eventos do mesmo cartão em 60 segundos. Configure o catálogo, região e credenciais no workspace Flink; execute os statements na ordem indicada em `sql/README.md`. O tópico de alertas é `payments.fraud.alerts`.

### 5. Operação

Use `scripts/ops-check.ps1` como checklist de ACLs, métricas e billing. Revise bytes recebidos, lag do consumidor, erros de connector, estado dos statements e taxa de alertas. A tabela de runbook está em [`docs/operations.md`](docs/operations.md); custo só deve ser reportado a partir do Billing API/console para o período e moeda informados.

## Preparação e execução do zero

Requisitos: conta Confluent Cloud com permissão para criar recursos, Terraform, Confluent CLI, PowerShell 7, PostgreSQL gerenciado acessível pelo conector, Schema Registry e Flink disponíveis na região escolhida.

1. Copie `.env.example` para `.env` e preencha localmente. Nunca versione `.env`.
2. Crie o banco PostgreSQL e habilite replicação lógica/TLS. Aplique `postgres/init/001_schema.sql` e `002_seed.sql` com `psql`.
3. Em `infra/terraform/`, inicialize, revise `terraform plan` e aplique. Guarde state e outputs sensíveis fora do Git.
4. Configure Schema Registry e registre schemas seguindo `schemas/README.md`.
5. Renderize o JSON do conector na memória/arquivo local ignorado pelo Git e crie o connector. Verifique status e eventos `c/u/d`.
6. Confirme que o connector criou os tópicos CDC, configure Flink e execute `sql/README.md` em ordem. O Flink Cloud expõe tópicos existentes automaticamente como tabelas.
7. Gere três eventos de teste no mesmo cartão dentro de 60 s; capture a saída do tópico de alerta e as métricas reais em `evidence/`.
8. Complete os itens de operação, custo e segurança no README, removendo os marcadores de captura apenas depois de anexar evidência verdadeira.

Comandos específicos do provider de PostgreSQL e do connector podem variar por versão. O arquivo de configuração e os exemplos devem ser conferidos com a documentação da versão disponibilizada no seu ambiente antes da criação.

Para validar o ciclo CDC sem mexer nos dados de seed, execute [`postgres/test_cdc.sql`](postgres/test_cdc.sql) enquanto o connector estiver ativo e capture cada evento antes da remoção.

## Resultado, performance e impacto no negócio

Resultado esperado: publicar alertas de triagem em streaming e permitir rastrear o dado até o evento CDC. Latência ponta a ponta, taxa de alerta, falsos positivos e custo mensal permanecem **não medidos** até uma execução real. Não há benchmark ou economia financeira fictícia neste repositório. Para business performance, estime `custo do risco residual = volume revisado × taxa de falso negativo × perda média por incidente`; substitua cada variável por métricas validadas pela área de risco.

## Evidências de execução

O PDF de referência pede evidência por camada. Use o checklist em [`evidence/README.md`](evidence/README.md) para coletar comandos, eventos e métricas reais. A pasta não contém evidências simuladas. Antes da submissão final, acrescente capturas higienizadas de: ambiente/cluster e ACLs, compatibilidade Schema Registry, eventos CDC insert/update/delete, alerta Flink mais as transações que o dispararam, métricas, billing e teardown vazio.

## Segurança e privacidade

- Segredos apenas em variáveis locais/secret manager; `.env`, state e arquivos renderizados estão ignorados.
- Service accounts separadas por produtor e consumidor com ACLs limitadas a tópicos/grupos necessários.
- TLS para PostgreSQL; usuário CDC dedicado; retenção e acesso revistos antes de produção.
- Dados sintéticos; não use PAN/CVV nem dados pessoais reais.
- Faça rotação e revogação das chaves após o laboratório.

## Custos e teardown

Não há preço fixo confiável sem região, plano, throughput e duração. Registre o custo observado no Billing para o intervalo do laboratório, moeda, componentes e eventuais créditos. Destrua recursos com `terraform destroy` depois de remover connector e statements; confirme que cluster, connector, tópicos e statements não permanecem ativos. Uma janela de billing pode continuar refletindo uso anterior após o teardown.

## Próximos passos

1. Adicionar DLQ e política de replay para eventos inválidos.
2. Avaliar outras regras com rótulos de fraude, medir precisão/recall e calibrar limiares com a equipe de risco.
3. Adicionar idempotência no consumidor e deduplicação usando chave do evento.
4. Materializar dados analíticos em Iceberg/Tableflow e aplicar política de retenção.
5. Adicionar score de risco versionado, com explicabilidade e monitoramento de drift.

## Licença

MIT. Consulte [`LICENSE`](LICENSE).

---

**Autor:** Sérgio Santos — Cientista de Dados | Ambientes Críticos e Governança de Dados

[![Portfólio Sérgio Santos](https://img.shields.io/badge/Portfólio-Sérgio_Santos-111827?style=for-the-badge&logo=githubpages&logoColor=00eaff)](https://portfoliosantossergio.vercel.app)
[![LinkedIn Sérgio Santos](https://img.shields.io/badge/LinkedIn-Sérgio_Santos-0A66C2?style=for-the-badge&logo=linkedin&logoColor=white)](https://linkedin.com/in/santossergioluiz)


