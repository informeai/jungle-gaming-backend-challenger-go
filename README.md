# Jungle Gaming — Processamento distribuído de apostas em Go

Serviço de carteira para provedores de jogos: API HTTP + consumidor SQS que movimentam carteiras com as mesmas garantias (idempotência persistente, ledger append-only, outbox transacional, inbox, locks por carteira), rodando com várias instâncias independentes.

- **Go 1.26.5** (`go.mod` e `Dockerfile`), **Uber Fx**, `net/http`, **pgx v5** com SQL explícito, **PostgreSQL 17**, **SQS** (LocalStack 4.7), **Keycloak 26** (OAuth 2.0/OIDC, `client_credentials`), Prometheus.
- Decisões técnicas, garantias e limitações: [`ARCHITECTURE.md`](ARCHITECTURE.md).

## Sumário

1. [Pré-requisitos](#pré-requisitos)
2. [Subindo o ambiente](#subindo-o-ambiente)
3. [Autenticação e identidades de teste](#autenticação-e-identidades-de-teste)
4. [Exemplos de chamadas](#exemplos-de-chamadas)
5. [Contrato HTTP](#contrato-http)
6. [Mensageria (SQS)](#mensageria-sqs)
7. [Migrations](#migrations)
8. [Variáveis de ambiente](#variáveis-de-ambiente)
9. [Testes](#testes)
10. [Observabilidade](#observabilidade)
11. [Estrutura do projeto](#estrutura-do-projeto)

## Pré-requisitos

- Docker + Docker Compose v2 (≈ 3 GB de RAM livres para Keycloak, LocalStack, Postgres e os 9 processos do serviço, ~10–25 MB cada).
- Go 1.26.5+ (apenas para rodar testes/ferramentas fora do container).
- `curl` e `python3` (usados pelos scripts de exemplo).
- Portas livres: `5432` (Postgres), `8180` (Keycloak), `4566` (LocalStack), `8081-8083` (API).

## Subindo o ambiente

```sh
docker compose up --build        # ou: make up (em background)
```

O Compose sobe, na ordem:

| Serviço | Porta | O que faz |
| --- | --- | --- |
| `postgres` | 5432 | Banco `wallet`; `deploy/postgres/init.sql` cria os papéis de runtime `wallet_app` e `wallet_relay` (os GRANTs vêm das migrations). |
| `keycloak` | 8180 | IdP. Importa automaticamente o realm `jungle` (`deploy/keycloak/jungle-realm.json`) com clientes, papéis e claims. Admin: `admin`/`admin`. |
| `localstack` | 4566 | SQS. `deploy/localstack/init-sqs.sh` cria as filas FIFO, as DLQs, a redrive policy e as políticas de acesso por identidade. |
| `migrate` | — | `wallet-service migrate up` como owner do banco; termina antes das APIs subirem. |
| `api-1..3` | 8081–8083 | API HTTP (3 processos). Papel `wallet_app`; **sem credencial AWS**. |
| `consumer-1..2` | interna | Consumidor SQS. Papel `wallet_app`; identidade AWS `wallet-consumer`. |
| `pending-worker-1..2` | interna | Worker de referências pendentes. Papel `wallet_app`; sem AWS. |
| `outbox-relay-1..2` | interna | Relay da outbox. Papel `wallet_relay` (só outbox); identidade AWS `wallet-outbox-relay`; **não recebe `DATABASE_URL`**. |

### Componentes separados com menor privilégio

É a **mesma imagem** com flags diferentes (`API_ENABLED`, `SQS_CONSUMER_ENABLED`, `PENDING_WORKER_ENABLED`, `OUTBOX_ENABLED`). Cada componente monta só as conexões e credenciais que usa, e os workers não publicam porta no host (servem `/health/*` e `/metrics` apenas na rede interna). Com todas as flags ligadas (padrão), o binário roda como um serviço único, útil fora do Docker. Motivação e modelo de privilégios: [`ARCHITECTURE.md`](ARCHITECTURE.md#componentes-e-menor-privilégio).

Todos os serviços têm healthcheck e só sobem com banco migrado, Keycloak e filas prontos. A readiness verifica apenas as dependências do componente:

```sh
curl -s localhost:8081/health/ready
# {"dependencies":{"postgres":"UP"},"status":"UP"}
docker compose exec outbox-relay-1 wget -qO- localhost:8080/health/ready
# {"dependencies":{"postgres-outbox":"UP","sqs-events":"UP"},"status":"UP"}
```

> Se você já tinha um volume do Postgres criado antes do papel `wallet_relay` existir, recrie-o: `docker compose down -v` (o `init.sql` só roda num volume vazio).

Para derrubar e limpar volumes: `docker compose down -v`.

### Rodando o binário fora do Docker

```sh
docker compose up -d --wait postgres keycloak localstack
set -a; source .env.example; set +a
go run ./cmd/wallet-service migrate up
go run ./cmd/wallet-service serve        # escuta em :8080
```

## Autenticação e identidades de teste

Todos os endpoints de negócio exigem `Authorization: Bearer <access_token>` emitido pelo Keycloak via `client_credentials`. O realm é provisionado automaticamente com:

| client_id | secret | Papel (realm role) | Claim `provider_id` | Pode |
| --- | --- | --- | --- | --- |
| `provider-a` | `provider-a-secret` | `wagering-provider` | `provider-a` | enviar operações e ler **apenas** transações do `provider-a` |
| `provider-b` | `provider-b-secret` | `wagering-provider` | `provider-b` | idem para `provider-b` |
| `wallet-internal` | `wallet-internal-secret` | `wallet-internal` | — | abrir/ler carteiras, ledger, reconciliação e ler qualquer transação |
| `reporting-no-role` | `reporting-no-role-secret` | — | — | nada (usado para testar 403) |
| `other-api-client` | `other-api-client-secret` | `wallet-internal` | — | nada: token sem audiência `wallet-api` (testa 401) |

Obter um token:

```sh
TOKEN=$(scripts/token.sh provider-a)              # usa o secret "<client>-secret"
# equivalente a:
curl -s -d grant_type=client_credentials -d client_id=provider-a -d client_secret=provider-a-secret \
  http://localhost:8180/realms/jungle/protocol/openid-connect/token
```

O `providerId` autorizado vem do token (claim `provider_id`), nunca do corpo da requisição.

## Exemplos de chamadas

O script `scripts/demo.sh` (ou `make demo`) executa o fluxo completo contra `http://localhost:8081`: abertura, aposta, replay idempotente, conflito, REFUND antes da aposta (pendente → resolvido), WIN, LOSS, ledger e reconciliação. Chamadas individuais:

```sh
INTERNAL=$(scripts/token.sh wallet-internal); PROVIDER=$(scripts/token.sh provider-a)
PLAYER=0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1

# Abrir carteira (serviço interno)
curl -s -X POST localhost:8081/wallets -H "Authorization: Bearer $INTERNAL" -H 'Content-Type: application/json' \
  -d "{\"playerId\":\"$PLAYER\",\"initialBalance\":{\"amount\":\"1000.00\",\"currency\":\"BRL\"}}"
# {"id":"<walletId>","playerId":"...","balance":{"amount":"1000.00","currency":"BRL"},"version":1}

WALLET=<walletId>

# Aposta (provedor) — Idempotency-Key obrigatório
curl -s -X POST localhost:8081/wagering/transactions -H "Authorization: Bearer $PROVIDER" \
  -H 'Content-Type: application/json' -H 'Idempotency-Key: provider-a:transaction-123' \
  -d "{\"providerId\":\"provider-a\",\"externalTransactionId\":\"transaction-123\",\"playerId\":\"$PLAYER\",
       \"walletId\":\"$WALLET\",\"roundId\":\"round-987\",\"gameId\":\"fortune-chimp\",\"kind\":\"BET\",
       \"money\":{\"amount\":\"25.00\",\"currency\":\"BRL\"}}"
# {"transactionId":"...","status":"PROCESSED","balance":{"amount":"975.00","currency":"BRL"},"idempotentReplay":false}

# Reversão: acrescente "referenceExternalTransactionId":"transaction-123" (REFUND/ROLLBACK)

# Consultas
curl -s localhost:8081/providers/provider-a/wagering/transactions/transaction-123 -H "Authorization: Bearer $PROVIDER"
curl -s localhost:8081/wagering/transactions/<transactionId> -H "Authorization: Bearer $PROVIDER"
curl -s localhost:8081/wallets/$WALLET -H "Authorization: Bearer $INTERNAL"
curl -s "localhost:8081/wallets/$WALLET/ledger?limit=50" -H "Authorization: Bearer $INTERNAL"   # use nextCursor
curl -s -X POST localhost:8081/wallets/$WALLET/reconciliation -H "Authorization: Bearer $INTERNAL"
```

## Contrato HTTP

| Método e rota | Quem | Descrição |
| --- | --- | --- |
| `POST /wallets` | interno | Abre carteira. Saldo positivo cria `OPENING` `PROCESSED`, lançamento de crédito e eventos no mesmo commit. `201`. |
| `GET /wallets/{walletId}` | interno | Saldo e versão. |
| `GET /wallets/{walletId}/ledger?cursor=&limit=` | interno | Lançamentos em ordem de inserção; `limit` 1–200 (padrão 50); `nextCursor` opaco (`null` na última página). |
| `POST /wallets/{walletId}/reconciliation` | interno | Recalcula o saldo pelo ledger num snapshot `REPEATABLE READ` somente leitura. |
| `POST /wagering/transactions` | provedor | Operação `BET`, `WIN`, `LOSS`, `REFUND`, `ROLLBACK`. Header `Idempotency-Key` obrigatório. |
| `GET /wagering/transactions/{transactionId}` | provedor (só as suas) ou interno | Estado, código de falha, tentativas e resultado. Transação de outro provedor → `404`. |
| `GET /providers/{providerId}/wagering/transactions/{externalTransactionId}` | o próprio provedor ou interno | Consulta pela chave externa. Outro provedor → `403`. |
| `GET /health/live`, `GET /health/ready` | público | Liveness do processo; readiness de Postgres e SQS (`503` se algum estiver fora ou durante o shutdown). |
| `GET /metrics` | público na rede interna | Métricas Prometheus. |

### Respostas de `POST /wagering/transactions`

Resultados persistidos (a operação existe no banco) sempre têm o corpo abaixo; `idempotentReplay` indica se veio de um processamento anterior:

```json
{"transactionId":"...","status":"PROCESSED","balance":{"amount":"975.00","currency":"BRL"},"idempotentReplay":false}
```

| HTTP | Situação | Corpo |
| --- | --- | --- |
| `200` | `PROCESSED` (novo ou replay; o replay devolve o saldo observado no processamento original) | resultado |
| `202` | `PENDING_REFERENCE` — aguardando a transação referenciada (acompanhe via GET) | resultado sem `balance` |
| `422` | `REJECTED` (regra de negócio, definitivo) ou `FAILED`; `failureCode` no corpo | resultado com `failureCode` |
| `400` | entrada inválida e corrigível; nada é persistido e a chave pode ser reutilizada | `{"error":{"code","message","field","correlationId"}}` |
| `401` | token ausente, inválido, de outra audiência (`UNAUTHENTICATED`) ou expirado (`TOKEN_EXPIRED`) | erro |
| `403` | papel insuficiente (`FORBIDDEN`) ou `providerId` diferente do token (`PROVIDER_MISMATCH`) | erro |
| `404` | `WALLET_NOT_FOUND` (nada persistido) | erro |
| `409` | `IDEMPOTENCY_KEY_CONFLICT` (mesma chave, conteúdo diferente) ou `EXTERNAL_TRANSACTION_CONFLICT` (mesmo `externalTransactionId` com outra chave) | erro |
| `503` | indisponibilidade transitória (banco, timeout); header `Retry-After`; repita com a **mesma** chave | erro `SERVICE_UNAVAILABLE` |
| `500` | erro inesperado | erro `INTERNAL_ERROR` |

Códigos de validação (`400`): `MALFORMED_REQUEST` (JSON inválido, campo desconhecido, `amount` numérico), `IDEMPOTENCY_KEY_REQUIRED`, `MISSING_FIELD`, `INVALID_FIELD`, `INVALID_KIND`, `OPENING_NOT_ALLOWED`, `INVALID_MONEY`, `AMOUNT_MUST_BE_POSITIVE`, `LOSS_AMOUNT_MUST_BE_ZERO`, `REFERENCE_REQUIRED`, `REFERENCE_NOT_ALLOWED`, `SELF_REFERENCE`, `INVALID_CURSOR`, `INVALID_LIMIT`.

`failureCode` de rejeições definitivas (`422`, persistidas): `INSUFFICIENT_FUNDS`, `INSUFFICIENT_FUNDS_FOR_REVERSAL`, `WALLET_PLAYER_MISMATCH`, `CURRENCY_MISMATCH`, `REFERENCE_NOT_FOUND`, `REFERENCE_STILL_PENDING`, `REFERENCE_NOT_PROCESSED`, `REFERENCE_MISMATCH`, `REFERENCE_AMOUNT_MISMATCH`, `REFERENCE_KIND_NOT_ALLOWED`, `REFERENCE_ALREADY_REVERSED`, `AMOUNT_OVERFLOW`. Falha permanente (`FAILED`): `PERMANENT_PROCESSING_ERROR`. O significado de cada um está em [`ARCHITECTURE.md`](ARCHITECTURE.md#códigos-de-falha).

## Mensageria (SQS)

Filas provisionadas automaticamente por `deploy/localstack/init-sqs.sh` (rodado pelo LocalStack quando fica pronto):

| Fila | Uso |
| --- | --- |
| `wager-transactions.fifo` | Entrada de operações. `VisibilityTimeout=30s`, long polling 10s, `RedrivePolicy` → DLQ com `maxReceiveCount=8`. |
| `wager-transactions-dlq.fifo` | Mensagens inválidas, conflitantes, de provedor não autorizado ou com tentativas esgotadas (atributo `dlqReason`). |
| `wallet-events.fifo` | Eventos de saída publicados pela outbox. |
| `wallet-events-dlq.fifo` | DLQ dos consumidores de eventos. |

Enviar uma operação pela fila (credenciais do produtor `provider-gateway`):

```sh
AWS_ACCESS_KEY_ID=provider-gateway AWS_SECRET_ACCESS_KEY=x AWS_DEFAULT_REGION=us-east-1 \
aws --endpoint-url http://localhost:4566 sqs send-message \
  --queue-url http://localhost:4566/000000000000/wager-transactions.fifo \
  --message-group-id "$WALLET" --message-deduplication-id msg-123 \
  --message-body "{\"messageId\":\"msg-123\",\"type\":\"WagerTransactionRequested\",\"occurredAt\":\"2026-09-08T12:00:00.000Z\",
    \"data\":{\"providerId\":\"provider-a\",\"externalTransactionId\":\"transaction-456\",\"idempotencyKey\":\"provider-a:transaction-456\",
    \"playerId\":\"$PLAYER\",\"walletId\":\"$WALLET\",\"roundId\":\"round-987\",\"gameId\":\"fortune-chimp\",\"kind\":\"BET\",
    \"money\":{\"amount\":\"25.00\",\"currency\":\"BRL\"}}}"
```

(sem AWS CLI: `docker compose exec localstack awslocal sqs send-message ...`).

Para ver o fluxo completo da fila de uma vez, rode `make sqs-demo` (script `scripts/sqs-demo.sh`, com a stack do compose no ar). Ele abre uma carteira, envia uma `BET` pela fila e espera o consumidor processar; reentrega a mesma mensagem (a inbox deduplica e o saldo não muda); repete a operação por HTTP (replay idempotente); envia uma mensagem inválida, que vai para a DLQ com o motivo; e lista os eventos que o relay publicou para a carteira. Contratos de `MessageGroupId`, `MessageDeduplicationId`, retries e DLQ: [`ARCHITECTURE.md`](ARCHITECTURE.md#consumidor-sqs).

## Migrations

Migrations versionadas em `migrations/` (formato golang-migrate, embutidas no binário), aplicadas pelo **owner** do banco; a aplicação roda com o papel restrito `wallet_app`.

```sh
# aplicar (o compose faz isso no serviço `migrate`)
MIGRATIONS_DATABASE_URL=postgres://postgres:postgres@localhost:5432/wallet?sslmode=disable \
  go run ./cmd/wallet-service migrate up
# reverter N passos (ou tudo, sem argumento)
... go run ./cmd/wallet-service migrate down 1
# versão atual
... go run ./cmd/wallet-service migrate version
# dentro do Docker
docker compose run --rm migrate migrate down 1
```

Atalhos: `make migrate-up`, `make migrate-down`. O teste `TestMigrationsUpDownUp` valida up → down → up num banco novo.

## Variáveis de ambiente

Todas têm valores de exemplo em [`.env.example`](.env.example). As principais:

| Variável | Padrão | Descrição |
| --- | --- | --- |
| `INSTANCE_ID` | hostname | Identidade da instância em logs e no lease da outbox. |
| `HTTP_ADDR` | `:8080` | Endereço HTTP. |
| `HTTP_REQUEST_TIMEOUT` / `HTTP_SHUTDOWN_TIMEOUT` | `10s` / `15s` | Prazo por requisição / para drenar requisições no shutdown. |
| `API_ENABLED`, `SQS_CONSUMER_ENABLED`, `PENDING_WORKER_ENABLED`, `OUTBOX_ENABLED` | `true` | Componentes ativos no processo (pelo menos um). |
| `DATABASE_URL` | — | Conexão `wallet_app`; obrigatória para API, consumidor e worker de pendências. |
| `OUTBOX_DATABASE_URL` | — | Conexão `wallet_relay`; obrigatória quando `OUTBOX_ENABLED=true`. |
| `MIGRATIONS_DATABASE_URL` | — | Conexão do owner usada por `migrate`. |
| `DATABASE_MAX_CONNS` | `20` | Tamanho do pool por instância. |
| `AWS_REGION`, `AWS_ENDPOINT_URL`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` | — | Acesso ao SQS/LocalStack. |
| `SQS_INGRESS_QUEUE`, `SQS_INGRESS_DLQ`, `SQS_EVENTS_QUEUE` | nomes acima | Filas (URLs resolvidas no start; ausência aborta a inicialização). |
| `SQS_VISIBILITY_TIMEOUT` / `SQS_HANDLER_TIMEOUT` | `30s` / `10s` | O prazo do handler precisa ser menor que a visibilidade (validado no start). |
| `SQS_MAX_RECEIVES` | `5` | Tentativas antes de mover para a DLQ. |
| `SQS_RETRY_BASE_DELAY` / `SQS_RETRY_MAX_DELAY` | `2s` / `60s` | Backoff exponencial (via visibility timeout) em falhas transitórias. |
| `SQS_CONCURRENCY` | `4` | Grupos FIFO processados em paralelo por instância. |
| `SQS_ALLOWED_PROVIDERS` | `provider-a,provider-b` | Provedores aceitos pela fila. |
| `OUTBOX_POLL_INTERVAL`, `OUTBOX_BATCH_SIZE`, `OUTBOX_LEASE`, `OUTBOX_BASE_BACKOFF`, `OUTBOX_MAX_BACKOFF` | `500ms`, `50`, `30s`, `1s`, `5m` | Relay da outbox. |
| `PENDING_WORKER_ENABLED`, `PENDING_POLL_INTERVAL`, `REFERENCE_MAX_ATTEMPTS`, `REFERENCE_BASE_DELAY`, `REFERENCE_MAX_DELAY`, `REFERENCE_TTL` | `true`, `1s`, `12`, `1s`, `1m`, `30m` | Referências pendentes. |
| `OIDC_ISSUER` | — (obrigatória com API) | Issuer esperado (`http://localhost:8180/realms/jungle`). |
| `OIDC_JWKS_URL` | `<issuer>/protocol/openid-connect/certs` | JWKS (no compose: `http://keycloak:8080/...`). |
| `OIDC_AUDIENCE` | `wallet-api` | Audiência exigida. |
| `OIDC_PROVIDER_CLAIM`, `OIDC_PROVIDER_ROLE`, `OIDC_INTERNAL_ROLE` | `provider_id`, `wagering-provider`, `wallet-internal` | Modelo de permissões. |
| `FAULT_INJECTION` | vazio | Somente testes: `consumer-crash-after-commit` ou `outbox-crash-after-publish`. |

## Testes

```sh
go test ./...          # unitários (sem infraestrutura)
go test -race ./...
go vet ./...
```

### Integração (infraestrutura real)

Os testes de integração ficam em `test/integration` com a build tag `integration` e usam **PostgreSQL, Keycloak e LocalStack reais** (nada de mocks):

```sh
docker compose up -d --wait postgres keycloak localstack      # ou: make deps
go test -tags integration -count=1 -v ./test/integration/...  # ou: make integration
go test -race -tags integration -count=1 ./test/integration/... # ou: make integration-race
```

Cada execução cria um banco novo (`it_<id>`, removido ao final; `IT_KEEP_DB=1` preserva) e aplica as migrations; cada teste de SQS cria suas próprias filas. O `TestMain` compila o binário com `-race` para os cenários com múltiplos processos, e qualquer data race nos processos filhos falha o teste. Variáveis opcionais: `IT_PG_ADMIN_URL`, `IT_PG_APP_USER`, `IT_PG_APP_PASSWORD`, `KEYCLOAK_URL`, `IT_AWS_ENDPOINT_URL`, `IT_LOG_LEVEL`.

| Teste | Cenário |
| --- | --- |
| `TestMigrationsUpDownUp` | Aplicação e reversão das migrations. |
| `TestLeastPrivilegeByComponent` | Com os papéis reais: o relay não lê carteiras/transações/ledger/inbox, não insere nem altera payload de eventos; `wallet_app` não lê, reivindica nem marca eventos como publicados. |
| `TestComponentsRunSeparately` | API, consumidor, worker e relay como 4 aplicações Fx separadas (o relay sem `DATABASE_URL`): fluxo HTTP + SQS + pendência + publicação; readiness por componente; workers sem rotas de negócio. |
| `TestSchemaInvariants` | Constraints e triggers: ledger imutável (UPDATE/DELETE/TRUNCATE), saldo não negativo, mudança de saldo sem ledger, versão, abertura duplicada, PENDING nunca commitado, papel `wallet_app` sem DELETE. |
| `TestAuthenticationWithKeycloak` | Token ausente, inválido, assinatura adulterada, outra audiência, expirado (token real do Keycloak); nenhuma movimentação. |
| `TestAuthorizationAndProviderIsolation` | Operações de carteira só para o interno; provedor não age nem lê como outro; replay isolado por provedor. |
| `TestSameBetFiftyTimesInParallel` | Mesma aposta 50× em paralelo → um débito, 49 replays. |
| `TestTwoConcurrentBetsOnSameWallet` | 100.00 com duas apostas de 80.00 → uma processada, uma `INSUFFICIENT_FUNDS`, saldo 20.00, um débito; reenvios não mudam nada. |
| `TestDistinctWalletsInParallel`, `TestNoGlobalLock` | Carteiras independentes em paralelo; uma carteira travada não bloqueia outra. |
| `TestHTTPContract` | Códigos HTTP, validações de dinheiro, conflitos, paginação do ledger, abertura com saldo zero, health. |
| `TestSQSConsumerInboxAndCrossTransport` | Inbox, reentrega, HTTP depois de SQS e vice-versa, rejeição de negócio terminal. |
| `TestSQSAndHTTPConcurrentSameOperation` | Mesma operação simultânea por HTTP (20×) e SQS (5×) → um débito. |
| `TestSQSPoisonMessagesGoToDLQ` | JSON inválido, provedor não autorizado, `OPENING`, hash divergente → DLQ com motivo. |
| `TestSQSTransientFailureRetriesThenDLQ` | Falha transitória → retry com backoff → DLQ após `SQS_MAX_RECEIVES`. |
| `TestConsumerCrashAfterCommitBeforeDelete` | Processo morre (exit 137) após o commit e antes do delete; outra instância recebe a reentrega e a inbox deduplica. |
| `TestOutboxCompetingPublishers` | Dois publishers disputando a mesma outbox: sem publicação dupla, 50 eventos distintos. |
| `TestOutboxRetryWithBackoff` | Falha de publicação → reagendamento com backoff → publicação posterior. |
| `TestOutboxRecoveryAfterCrash` | Eventos commitados sem relay + processo morto entre publicação e confirmação; outra instância reassume após o lease com o mesmo `eventId`. |
| `TestReversalBeforeReference` | REFUND (HTTP) e ROLLBACK (SQS) antes da aposta; resolução posterior; só uma compensação vence. |
| `TestPendingReferenceExpires` | Tentativas esgotadas → `REJECTED`/`REFERENCE_NOT_FOUND` + evento de rejeição. |
| `TestRestartPreservesState` | SIGKILL e novo processo: replay devolve o resultado original, pendência é retomada, saldo consistente. |
| `TestThreeIndependentProcesses` | Três processos: 50 duplicatas, disputa 80/80 entre processos distintos, carteiras em paralelo com HTTP + SQS simultâneos. |
| `TestFxLifecycle` | Composição Fx: start, workers ativos, stop, workers encerrados, pool fechado, servidor sem conexões. |
| `TestPostgresTemporarilyUnavailable` | Proxy TCP derruba o banco: `503` + readiness `DOWN`, nada persistido; após a volta, a mesma chave processa uma única vez. |

Unitários cobrem `Money` (parsing, escala, limites de `int64`, overflow, `NaN`/`Infinity`/notação científica, moedas incompatíveis, JSON), invariantes da carteira e do lançamento, máquina de estados, regras e política de zero dos cinco tipos externos, abertura interna (metadados e eventos), fingerprint canônico, conflito de payload para a mesma chave, inbox, validação de tokens (assinatura, issuer, audiência, expiração) e `fx.ValidateApp` do grafo.

### Múltiplas instâncias e simulação de falhas manualmente

```sh
docker compose up --build -d                 # 3 APIs (8081-8083) + 2 consumers, 2 relays, 2 pending workers
go run ./cmd/loadtest -wallets 50 -ops 5000 -concurrency 64 -duplicates 0.1
docker compose kill -s SIGKILL api-2 consumer-1 outbox-relay-1   # quedas abruptas; as réplicas seguem
docker compose stop api-3                     # SIGTERM: shutdown gracioso (ver logs)
docker compose start api-2 api-3 consumer-1 outbox-relay-1
```

Falhas específicas com `FAULT_INJECTION=consumer-crash-after-commit` ou `outbox-crash-after-publish` numa instância (ver testes correspondentes).

### Teste de carga

`cmd/loadtest` abre carteiras, dispara `BET`/`WIN` em paralelo contra as instâncias (com uma fração de reenvios), e imprime throughput, p50/p95/p99, códigos HTTP, conflitos de concorrência, duplicatas, atraso da outbox e reconcilia todas as carteiras ao final. Exemplo medido num Apple M1 (8 CPUs, Docker Desktop com 4 GB), 3 instâncias do compose:

```
operations:  5000 over 50 wallets, concurrency 64, duplicates 10%
elapsed:     3.574s
throughput:  1399.0 req/s
latency:     p50=37.8ms p95=100.5ms p99=177.6ms max=334.5ms
status:      map[200:5000]
metrics:     wallet_concurrency_conflicts_total 0 (nas 3 instâncias); duplicatas HTTP 134/146/162
outbox:      backlog de ~13k eventos no pico, drenado ~2s após o fim da carga
reconciliation: 50 wallets consistent
```

Não há meta de RPS; os números servem de linha de base e **variam bastante com o estado do host** (Docker Desktop). Ao separar os componentes, uma comparação A/B na mesma máquina e no mesmo momento deu 513–542 req/s para a versão anterior (tudo em um processo) e 600–700 req/s para a separada: a separação não reduziu o throughput. Com carga, a publicação da outbox compete por CPU com o LocalStack (~25% da variação medida) e o backlog drena ~5s após o fim.

## Observabilidade

- **Logs** JSON (`log/slog`) com `instanceId`, `correlationId` (header `X-Correlation-Id` ou gerado), `messageId`, `transactionId`, `walletId`, `providerId`, `eventId`. Corpos de requisição, tokens e payloads financeiros completos não são registrados.
- **Métricas** em `/metrics`: `wager_transactions_total{source,kind,status,replay}`, `wager_duplicates_total`, `wager_idempotency_conflicts_total`, `wallet_concurrency_conflicts_total`, `wager_processing_seconds`, `wager_reference_retries_total`, `sqs_messages_total{outcome}`, `sqs_message_retries_total`, `sqs_dlq_messages_total{reason}`, `outbox_published_total`, `outbox_publish_failures_total`, `outbox_pending_events`, `outbox_lag_seconds`, `outbox_publish_delay_seconds`, `wallet_reconciliations_total`, `wallet_reconciliation_divergences_total`, `http_requests_total`.
- **Health**: `/health/live` e `/health/ready`.

### Prometheus e Grafana (opcional)

```sh
make observability      # = docker compose --profile observability up --build -d
```

Sobe a stack completa mais:

| Serviço | URL | O que tem |
| --- | --- | --- |
| Prometheus | http://localhost:9090 | Coleta os 9 componentes a cada 5s pela rede interna (os workers não publicam porta), com o label `component`. Aba *Alerts* com as regras de `deploy/prometheus/alerts.yml`. |
| Grafana | http://localhost:3000 | Dashboard **Wallet Service** já provisionado (datasource e dashboard vêm de arquivos). Acesso anônimo como *Viewer*; admin: `admin`/`admin`. |

O dashboard tem filtro por componente e seis seções:
- **Visão geral:** operações/s, latência p95, atraso e backlog da outbox, mensagens na DLQ e divergências de saldo, estes três com cor de status.
- **Operações:** resultados por status, latência por origem e percentis, duplicatas e conflitos.
- **Mensageria:** resultados do consumidor, retries, DLQ e motivos de DLQ.
- **Outbox:** pendentes, atraso e publicações.
- **Referências e reconciliação.**
- **Saúde:** UP/DOWN de cada instância e requisições HTTP por classe de status.

Regras de alerta:

| Alerta | Condição |
| --- | --- |
| `ComponentDown` | uma instância deixou de responder à coleta por 30s |
| `NoOutboxRelayRunning` | nenhum relay no ar por 1 min |
| `OutboxLagHigh` | evento mais antigo esperando mais de 30s por 1 min |
| `MessagesSentToDLQ` | mensagem movida para a DLQ nos últimos 5 min |
| `ReconciliationDivergence` | divergência entre saldo e ledger em 15 min |
| `HighTransientErrorRate` | API respondendo 503 |

Não há Alertmanager configurado: localmente os alertas aparecem só na UI do Prometheus.

Para ver funcionando: rode `go run ./cmd/loadtest` e `make sqs-demo` com o dashboard aberto, ou pare os relays (`docker compose stop outbox-relay-1 outbox-relay-2`) e veja `ComponentDown` e `NoOutboxRelayRunning` dispararem em cerca de 1 min.

O Prometheus roda com `--enable-feature=created-timestamp-zero-ingestion`. Sem isso, o primeiro evento de um label novo (por exemplo, a primeira mensagem de um motivo de DLQ) seria tratado como linha de base, e `increase()` mostraria 0.

## Estrutura do projeto

```
cmd/wallet-service     binário: serve | migrate up|down|version
cmd/loadtest           teste de carga reprodutível
internal/domain        domínio puro: money, wallet (agregado + ledger), wagering (transações, regras, fingerprint), events
internal/app           casos de uso compartilhados por HTTP, SQS e workers; portas (interfaces)
internal/contract      formatos de fio compartilhados (JSON HTTP e envelope SQS)
internal/infra         adaptadores: postgres (pgx, repositórios, migrations), sqs (consumidor, publicador)
internal/httpapi       rotas net/http, autenticação/autorização, mapeamento de erros
internal/auth          validação OIDC (JWKS, issuer, audiência, expiração) e política de papéis
internal/worker        relay da outbox, worker de referências pendentes, loop com término observável
internal/bootstrap     composição Fx (fx.Module/Provide/Invoke + lifecycle)
internal/observability logger JSON e métricas
migrations/            SQL versionado (up/down)
deploy/                Keycloak realm, init do Postgres, provisionamento SQS, Prometheus (scrape + alertas) e Grafana (datasource + dashboard)
test/integration       testes com infraestrutura real (build tag integration)
```
