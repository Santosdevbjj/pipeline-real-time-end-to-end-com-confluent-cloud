# Conector CDC

O JSON é um modelo ilustrativo para o connector `PostgresCdcSourceV2`. A disponibilidade exata de propriedades depende da versão e do tipo de connector habilitado no cluster Confluent. Confirme os campos na tela/API de criação de connector e valide `database.sslmode`, `topic.prefix`, configuração de credenciais e ACLs para sua versão antes de criar. Não envie literalmente os marcadores `${file:...}`: injete secrets pelos campos de configuração segura da API/console.

Requisitos do banco: PostgreSQL suportado, `wal_level=logical`, slot/publication, TLS com certificado verificável, endpoint acessível pela rede do connector e usuário dedicado com permissão de replicação e SELECT nas tabelas. A configuração usa `publication.autocreate.mode=disabled`, então a publicação precisa existir antes do connector; veja `postgres/publication.sql`. Para CA privada em `verify-full`, envie `database.sslrootcert` em Base64 conforme exigido pela plataforma. Evite IP allowlist aberta. Defina limites de retenção WAL e monitore slots inativos para evitar crescimento do disco.

Tópicos normalmente seguem `<topic.prefix>.<schema>.<table>`; confirme o nome observado e atualize `sql/00_sources.sql` se o nome variar.
