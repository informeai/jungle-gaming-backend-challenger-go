# Arquitetura e decisões

Este documento registra as decisões técnicas, as garantias e como cada uma é imposta, as interpretações adotadas e as limitações conhecidas.

## Visão geral

```
              HTTP (JWT Keycloak)                 SQS wager-transactions.fifo
                     │                                        │
              internal/httpapi                     internal/infra/sqs (Consumer)
                     │  contract.WagerPayload → app.ParseSubmit (mesma validação + fingerprint)
                     └──────────────┬─────────────────────────┘
                                    ▼
                       internal/app (WageringService / WalletService)
                       │ 1 transação SQL: [inbox] + transação + carteira + ledger + outbox
                       ▼
                 PostgreSQL (constraints + triggers)
                       ▲                     ▲
      worker de referências pendentes     relay da outbox ──► SQS wallet-events.fifo
```

Camadas e dependências (de fora para dentro):

- `internal/domain` — domínio puro. Não conhece Fx, HTTP, SQS nem pgx (só `google/uuid`). Entidades com estado encapsulado, construtores validados, reidratação separada da criação, erros classificáveis (`errors.Is/As`), sem `panic` para regras de negócio.
- `internal/app` — casos de uso e **portas** (interfaces de repositório, TxManager, métricas). É o único caminho para movimentar dinheiro; HTTP, SQS e workers chamam os mesmos métodos.
- `internal/infra/postgres`, `internal/infra/sqs`, `internal/httpapi`, `internal/auth`, `internal/worker` — adaptadores.
- `internal/bootstrap` — raiz de composição com Uber Fx.

## Dinheiro

- **Representação:** `money.Money{minor int64, currency Currency}` — unidades mínimas (centavos) com **escala fixa de 2 casas**. Intervalo: `[-92 233 720 368 547 758.08, 92 233 720 368 547 758.07]`.
- **Moedas:** ISO 4217 de 3 letras maiúsculas, restritas às que têm 2 casas decimais (`BRL`, `USD`, `EUR`, `GBP`, `MXN`, `ARS`). Moedas sem 2 casas (ex.: `JPY`) são rejeitadas para não quebrar a escala fixa. O cenário principal usa BRL; testes cobrem incompatibilidade entre moedas.
- **Parsing** dígito a dígito, sem `float` em nenhum ponto: gramática `-?(0|[1-9][0-9]*)\.[0-9]{2}`. Rejeita vazio, espaços, `+`, `NaN`, `Infinity`, notação científica, escala ausente ou excedente, zeros à esquerda, separador de milhar. **Nenhuma normalização**: só a forma canônica é aceita, então não há arredondamento silencioso e o valor usado no hash é exatamente o recebido.
- Entradas financeiras externas usam `ParseNonNegative` (rejeita `-`, inclusive `-0.00`). Valores negativos existem apenas internamente (ex.: `difference` da reconciliação).
- No JSON, `amount` precisa ser **string**; número JSON é erro de decodificação (o campo é `*string`, então o `encoding/json` nunca converte para float).
- **Overflow** verificado em parsing, soma, subtração e negação (`ErrOverflow`). Um crédito que estouraria o saldo é rejeitado com `AMOUNT_OVERFLOW`.
- Operações exigem moedas iguais (`ErrCurrencyMismatch`) e valores inicializados (`ErrUninitialized`; o zero-value de `Money` é inválido).
- **Persistência:** `BIGINT` em unidades mínimas + `CHAR(3)` da moeda em todas as tabelas (`balance_minor`, `amount_minor`, `balance_before_minor`...). Somas no banco (`SUM(bigint)` → `numeric`) são convertidas explicitamente de volta para `bigint`.

## Persistência e transações

- **Biblioteca:** `pgx/v5` com SQL explícito (sem ORM). Locks, constraints e isolamento ficam visíveis no código (`internal/infra/postgres`).
- **Delimitação da transação:** `app.TxManager.WithinTx(ctx, fn)` abre uma transação `READ COMMITTED` e a coloca no `context.Context`; todo repositório chamado com esse `ctx` usa a mesma `pgx.Tx`. Chamadas aninhadas reutilizam a transação externa — é assim que o consumidor SQS junta **inbox + caso de uso + inbox concluída** num único commit, e o caso de uso HTTP junta **transação + carteira + ledger + outbox**. `WithinSnapshot` abre `REPEATABLE READ READ ONLY` para a reconciliação.
- **Classificação de erros:** `postgres.Classify` converte falhas de conexão, timeout, `40001`/`40P01` (serialização/deadlock), `55P03`, `53300` e classe `57` em `app.TransientError`. Violações de constraint e demais erros são permanentes.
- **Retry de transação:** a entrada HTTP e o worker repetem a transação inteira até 3 vezes em erro transitório ou `ErrConcurrentUpdate`, com backoff curto; o SQS usa a própria reentrega.

### Schema (migration `000001`)

| Tabela | Destaques |
| --- | --- |
| `wallets` | `UNIQUE(player_id, currency)`, `CHECK balance_minor >= 0`, `CHECK version >= 1`. |
| `wager_transactions` | `CHECK` do formato por origem (INTERNAL = só `OPENING` sem metadados externos; EXTERNAL = nunca `OPENING`, com provedor, id externo, chave, hash, rodada e jogo), política de zero (`LOSS = 0`, demais `> 0`), reversão exige referência, `REJECTED/FAILED` exigem `failure_code`, `PROCESSED` exige saldo resultante. Índices únicos parciais: `(provider_id, external_transaction_id)`, `(provider_id, idempotency_key)`, uma `OPENING` por carteira, uma reversão processada por referência. |
| `wallet_ledger_entries` | `UNIQUE(wallet_id, transaction_id)`, `CHECK` aritmético `after = before ± amount`, valores não negativos, `seq` identity para ordenação estável. |
| `inbox_messages` | PK `(consumer_name, message_id)`, hash, recebimento e conclusão. |
| `outbox_events` | `id` = `eventId` estável, agregado, tipo, versão, `payload jsonb`, `occurred_at`, `attempts`, `next_attempt_at`, `locked_by/locked_until` (lease), `published_at`, `last_error`. |

Triggers (proteção independente da aplicação):

- Ledger: `UPDATE`, `DELETE` e `TRUNCATE` proibidos (append-only). Carteiras, transações, inbox e outbox não podem ser apagadas.
- `wallets_guard_update`: identidade imutável; a versão sobe **exatamente 1** quando o saldo muda e não muda caso contrário.
- `wallets_require_ledger` (*constraint trigger* `DEFERRABLE INITIALLY DEFERRED`): no commit, toda mudança de saldo (e toda carteira criada com saldo) precisa de um lançamento **da mesma transação** (`xmin = pg_current_xact_id()`) indo do saldo antigo ao novo. Impossível alterar saldo sem ledger, mesmo por SQL manual.
- `wager_guard_update`: estados terminais são finais; campos de negócio (valor, tipo, carteira, provedor, chave, hash...) são imutáveis; nada volta para `PENDING`.
- `wager_no_committed_pending` (deferred): uma transação nunca é commitada em `PENDING`.
- `outbox_guard_update`: o snapshot do evento é imutável e `published_at` não pode ser desfeito.
- **Menor privilégio:** a aplicação conecta como `wallet_app`, que só tem `SELECT/INSERT/UPDATE` (e apenas `SELECT/INSERT` no ledger). Migrations rodam como owner.

## Carteira e controle de concorrência

- `Wallet` é a raiz do agregado: `Debit`/`Credit` validam moeda, valor positivo e saldo, devolvem o `LedgerEntry` correspondente e incrementam a versão. `Open` cria na versão 1 (com o crédito de abertura já embutido, sem incrementar); `Rehydrate` apenas reconstrói e valida.
- **Estratégia: lock pessimista por carteira + compare-and-set de versão + constraints.**
  1. `SELECT ... FROM wallets WHERE id = $1 FOR UPDATE` — serializa as operações **da mesma carteira** entre todos os processos. Não há lock global: carteiras diferentes travam linhas diferentes e seguem em paralelo (`TestNoGlobalLock`).
  2. `UPDATE wallets ... WHERE id = $1 AND version = $expected` — defesa em profundidade contra lost update; 0 linhas → `ErrConcurrentUpdate` → retry.
  3. `CHECK balance_minor >= 0` e os triggers acima garantem as invariantes mesmo que a aplicação falhe.
- Ordem de locks sempre **carteira → linha da transação**, evitando deadlocks entre o fluxo síncrono e o worker.
- Por que pessimista: a contenção é por carteira e as transações são curtas; o lock evita retries em cascata na disputa das duas apostas de 80.00 e torna o resultado determinístico (a segunda vê o saldo já debitado e é rejeitada com `INSUFFICIENT_FUNDS`). No teste de carga, `wallet_concurrency_conflicts_total` ficou em 0.

## Transações de aposta (WagerTransaction)

### Máquina de estados

```
PENDING ──► PROCESSED | REJECTED | FAILED | PENDING_REFERENCE
PENDING_REFERENCE ──► PENDING_REFERENCE (reagendamento) | PROCESSED | REJECTED | FAILED
PROCESSED, REJECTED, FAILED: terminais (validado no domínio e por trigger)
```

- `PENDING` é **transitório dentro da transação SQL**: a operação é inserida e liquidada no mesmo commit (não há aceite assíncrono). Por isso não existe `PENDING` commitado para retomar (o banco impede via trigger); o único estado de espera durável é `PENDING_REFERENCE`, retomado pelo worker em qualquer instância.
- Replay de transação terminal devolve o resultado persistido (`result_balance_minor`, `failure_code`), sem reaplicar.

### Falhas transitórias × permanentes

- **Transitórias** (banco/SQS indisponível, timeout, serialização, deadlock): nada é commitado; HTTP responde `503` com `Retry-After` (reenvie com a mesma chave); SQS volta a mensagem para a fila com backoff; o worker tenta no próximo ciclo.
- **Entradas inválidas corrigíveis** (validação, carteira inexistente): não persistem nada; HTTP `400/404`; SQS → DLQ.
- **Rejeições de negócio definitivas:** persistidas como `REJECTED` com `failureCode` e evento `WagerTransactionRejected`; replays devolvem a mesma rejeição.
- **Falha permanente de processamento** (erro não transitório inesperado ao retomar uma pendência, ex.: dado corrompido): a transação vai para `FAILED` com `PERMANENT_PROCESSING_ERROR` para auditoria, em vez de ficar em loop.

### Regras por tipo

| Tipo | Movimento | Regras |
| --- | --- | --- |
| `BET` | débito | valor > 0; sem referência; saldo insuficiente → `INSUFFICIENT_FUNDS`. |
| `WIN` | crédito | valor > 0; referência opcional a uma `BET` processada da mesma rodada/provedor/jogador/carteira/moeda. |
| `LOSS` | nenhum | valor exatamente `0.00`; sem ledger, sem mudança de versão; emite só `WagerTransactionProcessed`. |
| `REFUND` | crédito | referência obrigatória a uma `BET` processada; valor igual ao da aposta. |
| `ROLLBACK` | contrário ao original | referência a `BET` (crédito), `WIN` ou `REFUND` (débito); valor igual. Débito sem saldo → `INSUFFICIENT_FUNDS_FOR_REVERSAL` (distinto de `INSUFFICIENT_FUNDS`). |
| `OPENING` | crédito | apenas interno (abertura); rejeitado por HTTP/SQS com `OPENING_NOT_ALLOWED`. |

Referência resolvida por `(providerId, referenceExternalTransactionId)` e precisa concordar em provedor, jogador, carteira, moeda e rodada (`REFERENCE_MISMATCH`).

### Combinações de REFUND e ROLLBACK

Regra: **uma referência recebe no máximo uma compensação bem-sucedida**, seja `REFUND` ou `ROLLBACK` (índice único parcial `wager_single_reversal_per_reference` + verificação no domínio sob o lock da carteira).

- `REFUND` e `ROLLBACK` da mesma `BET`: o primeiro processado vence; o outro é `REJECTED` com `REFERENCE_ALREADY_REVERSED`. Nunca há devolução dupla do mesmo débito (testado inclusive com um chegando por HTTP e outro por SQS, ambos antes da aposta).
- `ROLLBACK` de um `REFUND`: permitido uma vez (debita a devolução). Depois disso a aposta volta a valer como débito, e **não** pode ser reembolsada de novo, porque ela já tem uma compensação processada. É uma escolha conservadora: impede ciclos refund→rollback→refund.
- `ROLLBACK` de `ROLLBACK` e `REFUND` de algo que não seja `BET`: `REFERENCE_KIND_NOT_ALLOWED`.
- Reversões parciais não são suportadas (`REFERENCE_AMOUNT_MISMATCH`).

### Referências ainda indisponíveis

- Referência inexistente **ou** ainda não terminal (`PENDING_REFERENCE`) → a operação vai para `PENDING_REFERENCE` com `attempts = 1`, `next_attempt_at = now + base` e `expires_at = now + TTL`, e emite `WagerTransactionPendingReference`. HTTP responde `202`.
- O worker (`internal/worker/pending.go`) consulta `status = 'PENDING_REFERENCE' AND next_attempt_at <= now()`, trava a carteira, relê a transação com `FOR UPDATE`, confere que ainda está pendente e vencida, e reavalia. Qualquer instância pode fazer isso; o estado está só no banco, então sobrevive a reinícios (`TestRestartPreservesState`).
- Backoff exponencial: `base × 2^n` limitado a `REFERENCE_MAX_DELAY` (padrão 1s, 2s, 4s... até 1min).
- Quando a referência chega e termina, `WakeDependents` antecipa `next_attempt_at` das operações que esperam por ela, então a resolução acontece em milissegundos.
- **Expiração:** após `REFERENCE_MAX_ATTEMPTS` avaliações (padrão 12, contando a primeira) ou `REFERENCE_TTL` (padrão 30 min) → `REJECTED` com `REFERENCE_NOT_FOUND` (nunca chegou) ou `REFERENCE_STILL_PENDING` (existe mas não terminou) + `WagerTransactionRejected`.
- Referência que terminou sem sucesso (`REJECTED`/`FAILED`) → `REJECTED` imediato com `REFERENCE_NOT_PROCESSED`.

### Códigos de falha

| Código | Tipo | Significado |
| --- | --- | --- |
| `INSUFFICIENT_FUNDS` | definitivo | Aposta maior que o saldo. |
| `INSUFFICIENT_FUNDS_FOR_REVERSAL` | definitivo | Reversão que precisaria debitar mais que o saldo. |
| `WALLET_PLAYER_MISMATCH` | definitivo | A carteira não pertence ao `playerId` informado (o saldo não é exposto). |
| `CURRENCY_MISMATCH` | definitivo | Moeda da operação diferente da carteira. |
| `REFERENCE_NOT_FOUND` | definitivo | Referência não chegou dentro do limite de tentativas/TTL. |
| `REFERENCE_STILL_PENDING` | definitivo | Referência existe mas continuou pendente até o limite. |
| `REFERENCE_NOT_PROCESSED` | definitivo | Referência terminou `REJECTED`/`FAILED`. |
| `REFERENCE_MISMATCH` | definitivo | Referência de outro provedor/jogador/carteira/moeda/rodada. |
| `REFERENCE_AMOUNT_MISMATCH` | definitivo | Reversão com valor diferente do original. |
| `REFERENCE_KIND_NOT_ALLOWED` | definitivo | Tipo referenciado não pode ser compensado por esta operação. |
| `REFERENCE_ALREADY_REVERSED` | definitivo | A referência já tem uma compensação processada. |
| `AMOUNT_OVERFLOW` | definitivo | Crédito estouraria o limite de `int64`. |
| `PERMANENT_PROCESSING_ERROR` | `FAILED` | Falha permanente de infraestrutura registrada para auditoria. |

Códigos de validação (`400`, corrigíveis, não persistidos): ver README.

## Idempotência

- **Persistente:** índices únicos `(provider_id, idempotency_key)` e `(provider_id, external_transaction_id)` em `wager_transactions`. Sobrevive a reinícios de todos os processos (`TestRestartPreservesState`). Nada de estado em memória.
- **Escopo por provedor:** a chave é única por provedor, então um provedor não consegue ler ou colidir com o resultado de outro reutilizando a mesma chave (`TestAuthorizationAndProviderIsolation`).
- **Fluxo:** (1) busca pela chave → se existe, compara o hash; igual = replay (`idempotentReplay: true`, mesmo `transactionId`, saldo observado no processamento original), diferente = `409 IDEMPOTENCY_KEY_CONFLICT`. (2) Trava a carteira. (3) `INSERT ... ON CONFLICT DO NOTHING` — uma inserção concorrente da mesma chave espera a primeira terminar; se não inseriu, relê: mesma chave → replay/conflito; mesmo `externalTransactionId` com outra chave → `409 EXTERNAL_TRANSACTION_CONFLICT` (a operação nunca é reaplicada com outra chave). O servidor nunca troca a chave recebida por uma calculada.
- **Hash (fingerprint):** SHA-256 hex do **JSON canônico** dos campos de negócio: `providerId`, `externalTransactionId`, `playerId`, `walletId`, `roundId`, `gameId`, `kind`, `money{amount,currency}` e `referenceExternalTransactionId` (omitido quando vazio). Chaves ordenadas lexicograficamente (`encoding/json` ordena mapas), sem espaços, UUIDs em minúsculas na forma canônica, `amount` na forma canônica de 2 casas (como não há normalização de entrada, é o próprio texto recebido). Excluídos: chave de idempotência, headers, envelope SQS (`messageId`, `type`, `occurredAt`) e `data.idempotencyKey`. HTTP e SQS passam pelo mesmo `app.ParseSubmit`, então a mesma operação gera o mesmo hash nos dois transportes (teste unitário fixa a forma canônica documentada).

## Inbox e consumidor SQS

- **Identidade durável** da mensagem: `messageId` do envelope (não o `MessageId` do SQS). Registro em `inbox_messages (consumer_name, message_id)` com o hash.
- **Atomicidade:** `inbox.Register` + caso de uso + `inbox.Complete` numa única transação SQL, junto com carteira, ledger, transação e outbox. A mensagem só é removida da fila **depois do commit**.
- **Reentrega:** mesmo `messageId` e hash → duplicata, removida sem efeito; hash diferente → DLQ (`message_hash_mismatch`). A mesma operação com outro `messageId` cai na idempotência financeira (replay).
- Referência pendente: a mensagem é concluída depois que `PENDING_REFERENCE` foi persistido; o worker assume a continuidade.
- **Crash depois do commit e antes do delete:** a mensagem reaparece após o visibility timeout, a inbox detecta a duplicata e ela é removida (`TestConsumerCrashAfterCommitBeforeDelete`, com o processo morto de verdade via `FAULT_INJECTION`).

### Consumidor SQS

| Situação | Tratamento |
| --- | --- |
| Sucesso ou rejeição de negócio commitada | `DeleteMessage`. Se o delete falhar, a reentrega é deduplicada. |
| Envelope inválido, tipo desconhecido, validação, `OPENING`, provedor fora de `SQS_ALLOWED_PROVIDERS`, carteira inexistente, conflito de idempotência, hash divergente | Permanente: enviada à DLQ com atributos `dlqReason`, `sourceQueue`, `receiveCount` e removida da fila de entrada. |
| Falha transitória | `ChangeMessageVisibility` com backoff exponencial (`SQS_RETRY_BASE_DELAY × 2^(n-1)`, até `SQS_RETRY_MAX_DELAY`), usando `ApproximateReceiveCount`. |
| Tentativas esgotadas (`receiveCount >= SQS_MAX_RECEIVES`, padrão 5) | DLQ com `dlqReason=retries_exhausted`. |
| Rede de segurança | `RedrivePolicy` da fila com `maxReceiveCount=8` (> `SQS_MAX_RECEIVES`) para o caso de um consumidor morrer sempre na mesma mensagem. |

- **Visibility timeout** 30s; **prazo do handler** 10s (validado no start: precisa ser menor que a visibilidade, para a mensagem nunca ficar visível enquanto ainda é processada).
- **`MessageGroupId`** = `walletId`: ordem FIFO por carteira e paralelismo entre carteiras. O consumidor processa mensagens do mesmo grupo **em sequência** e grupos diferentes em paralelo (`SQS_CONCURRENCY`); se uma mensagem do grupo vai para retry, as seguintes do mesmo lote são devolvidas para preservar a ordem.
- **`MessageDeduplicationId`** = recomendado igual ao `messageId` do envelope (deduplicação do broker por 5 min). A deduplicação do SQS FIFO é só uma otimização: a correção vem da inbox e da idempotência no banco.
- **SIGTERM:** o consumidor para de buscar (cancela o long polling), espera o trabalho em andamento até o prazo do shutdown; o que não terminou é cancelado (rollback) e tem a visibilidade liberada (`VisibilityTimeout=0`) para reentrega imediata em outra instância.
- **Indisponibilidade do SQS:** o loop de recebimento faz backoff exponencial (até 10s) e a readiness passa a `503`. A outbox acumula e é drenada quando o SQS volta.

## Transactional outbox

- Os eventos são gravados em `outbox_events` **na mesma transação** que os originou; nada é publicado antes do commit.
- **Relay** (`internal/worker/outbox.go`) roda em todas as instâncias:
  - *Claim*: `UPDATE ... SET locked_by = <instância>, locked_until = now() + lease, attempts = attempts + 1 WHERE id IN (SELECT ... WHERE published_at IS NULL AND next_attempt_at <= now() AND (locked_until IS NULL OR locked_until < now()) ORDER BY occurred_at FOR UPDATE SKIP LOCKED LIMIT n)`. Vários publishers disputam sem publicar o mesmo evento em paralelo.
  - Publica com `MessageGroupId = walletId` e `MessageDeduplicationId = eventId`; confirma com `published_at` somente se ainda detém o lease.
  - Falha → libera o lease e agenda `next_attempt_at = now + base × 2^(attempts-1)` (até `OUTBOX_MAX_BACKOFF`), gravando `last_error`.
  - **Trabalho abandonado:** se a instância morrer com o lease, outra reassume depois de `OUTBOX_LEASE`.
- **Recuperação:** entre commit e publicação, o evento simplesmente continua pendente e qualquer relay o pega. Entre publicação e confirmação, ele é republicado **com o mesmo `eventId`** após o lease (`TestOutboxRecoveryAfterCrash`). Entrega **at-least-once**.
- O payload é um **snapshot imutável** (trigger impede alteração); republicar envia exatamente o mesmo conteúdo.

### Eventos e contrato de consumo

Destino: `wallet-events.fifo` (DLQ `wallet-events-dlq.fifo`). Atributos de mensagem: `eventType`, `eventId`, `correlationId`.

```json
{
  "eventId": "uuid", "eventType": "WalletBalanceChanged", "aggregateId": "uuid",
  "correlationId": "string", "causationId": "string (opcional)",
  "occurredAt": "2026-09-08T12:00:00.000Z", "version": 1,
  "data": { "walletId": "...", "transactionId": "...", "direction": "DEBIT",
            "money": {"amount":"25.00","currency":"BRL"},
            "balanceBefore": {...}, "balanceAfter": {...}, "walletVersion": 2 }
}
```

| Evento | Agregado | Gatilho |
| --- | --- | --- |
| `WagerTransactionProcessed` | transação | Conclusão com sucesso (inclui `LOSS` e `OPENING`); traz saldo e versão resultantes. |
| `WagerTransactionRejected` | transação | Rejeição definitiva, com `failureCode`. |
| `WalletBalanceChanged` | carteira | Toda mudança efetiva de saldo. |
| `WagerTransactionPendingReference` | transação | Início da espera pela referência (com `nextAttemptAt`/`expiresAt`). |

Tipo e versão são fixados pelos construtores em `internal/domain/events`; datas em RFC 3339 UTC; dinheiro em string decimal. `correlationId` é o `X-Correlation-Id` do HTTP ou o `messageId` do SQS; `causationId` é o `messageId` que originou a operação (quando veio pela fila).

**Contrato para consumidores:** deduplicar por `eventId` (republicações fora da janela de 5 min do SQS são possíveis); ordenar por carteira usando `walletVersion` (a ordem de publicação entre relays diferentes pode divergir da ordem de commit — ver limitações).

## Autenticação e autorização

- **IdP: Keycloak** (realm `jungle`, provisionado por import no Compose). Escolhido por ser OIDC completo, open source, com `client_credentials`, mappers declarativos (claims fixas, audiência) e import de realm reproduzível. Emissão de token e cadastro de credenciais ficam inteiramente no IdP.
- **Validação** (`internal/auth`, `coreos/go-oidc`): assinatura via **JWKS** (chaves buscadas e cacheadas, com rotação), `iss` exato, audiência `wallet-api` obrigatória (mapper de audiência no Keycloak), expiração sem tolerância, algoritmos RS256/ES256/PS256. Issuer e endereço do JWKS são configuráveis separadamente (no Compose o token diz `localhost:8180` e a API busca as chaves em `keycloak:8080`).
- **Modelo de permissões** (papéis de realm + claim):
  - `wagering-provider` + claim `provider_id` (mapper *hardcoded claim* por cliente): envia operações e lê as próprias transações. O `providerId` **vem do token**; corpo com outro provedor → `403 PROVIDER_MISMATCH` sem efeito. Leitura por id de transação de outro provedor → `404` (não revela existência); pela rota `/providers/{id}` de outro → `403`. A idempotência é escopada por provedor, então replays também são isolados.
  - `wallet-internal`: operações de carteira (abrir, consultar, ledger, reconciliação) e leitura de qualquer transação. Não envia operações de provedor.
  - Token válido sem papel → `403`. Token ausente/inválido/expirado/de outra audiência → `401`. Nenhum desses casos toca o banco.
- **Mensageria:** o acesso ao broker é por credenciais e política da fila: o produtor (`provider-gateway`) só tem `SendMessage` na fila de entrada; o serviço (`wallet-service`) só consome. As políticas são aplicadas pelo `init-sqs.sh`. **O LocalStack Community não aplica IAM**, então localmente elas são declarativas (na AWS, seriam aplicadas). Por isso o consumidor mantém as validações de domínio: envelope estrito, provedor permitido (`SQS_ALLOWED_PROVIDERS`), mesmas regras e idempotência do HTTP.

## Uso do Fx e ciclo de vida

- `internal/bootstrap` compõe módulos com `fx.Module`: `observability`, `postgres`, `sqs`, `auth`, `app`, `workers`, `http`. Tudo por construtores (`fx.Provide`); portas ligadas às implementações por construtores adaptadores; `fx.Invoke` ativa workers e servidor. `fx.ValidateApp` roda nos testes unitários.
- **Inicialização:** `config.Load` valida a configuração (obrigatórias, limites, `handler timeout < visibility timeout`) antes do Fx. Os hooks `OnStart` validam dependências: ping no Postgres, resolução das URLs das filas (falha se alguma não existir), listen do HTTP (erro de bind aborta o start). Workers começam depois das dependências.
- **Workers** (`worker.Loop`): goroutine com contexto cancelável e canal `done`; `Stop` cancela, espera o término dentro do prazo e registra; `Running()` expõe o estado (usado em `TestFxLifecycle`).
- **Shutdown** (ordem inversa, verificada nos logs do compose):
  1. HTTP: readiness passa a `503` (drain), `Server.Shutdown` para de aceitar conexões e aguarda as requisições em andamento.
  2. Consumidor SQS: para de buscar mensagens, conclui o que está em andamento ou cancela e libera a visibilidade.
  3. Worker de referências e relay da outbox: param entre ciclos; leases não usados são devolvidos.
  4. Pool do Postgres é fechado por último.
- Prazos: `HTTP_SHUTDOWN_TIMEOUT`, `SQS_HANDLER_TIMEOUT` e `fx.StopTimeout` (soma de ambos + margem); `stop_grace_period: 40s` no Compose.

## Observabilidade

Logs JSON com identificadores de rastreio, métricas Prometheus (resultado por status, duplicatas, conflitos de idempotência e concorrência, retries de SQS e referências, DLQ por motivo, atraso e backlog da outbox, latência, divergências de reconciliação) e health checks. A reconciliação registra `ERROR` e incrementa `wallet_reconciliation_divergences_total` quando há diferença; ela nunca altera saldo.

## Interpretações adotadas

- **Aceite síncrono:** operações sem dependências são concluídas na própria requisição/mensagem, sem commit intermediário de `PENDING`. A única espera durável é `PENDING_REFERENCE`.
- **Carteira inexistente** é erro corrigível (`404` / DLQ), não rejeição persistida — não existe carteira para associar a transação (FK).
- **`WALLET_PLAYER_MISMATCH`/`CURRENCY_MISMATCH`** são rejeições persistidas: a operação fica consultável e um reenvio devolve a mesma rejeição (replay).
- **Provedores × carteiras:** qualquer provedor autenticado pode operar em qualquer carteira (a carteira é do jogador, não do provedor); o isolamento é das transações de cada provedor.
- **`WIN` com referência** usa a mesma espera de referência de `REFUND`/`ROLLBACK`; não impede vários `WIN` para a mesma aposta.
- **`ROLLBACK` de uma `BET` que já teve `WIN`** é permitido (cada compensação é independente); coerência entre eles é responsabilidade do provedor.
- **Reconciliação** compara o saldo com `Σ créditos − Σ débitos` do ledger, incluindo a abertura, no mesmo snapshot.
- **`/metrics`** é público assumindo rede interna/scrape; em produção ficaria atrás de rede privada.

## Limitações e trabalho não concluído

- **IAM do SQS** não é aplicado pelo LocalStack Community (ver acima).
- **Ordem global de eventos por carteira** não é estrita com vários relays: o grupo FIFO preserva a ordem de *publicação*, que pode diferir da ordem de commit quando dois relays publicam eventos da mesma carteira ao mesmo tempo. Consumidores devem usar `walletVersion`. Uma alternativa seria reivindicar só o evento mais antigo pendente de cada `partition_key`.
- **Retenção** de inbox e outbox publicadas não é feita (sem job de limpeza/particionamento).
- **Partidas dobradas**, **tracing OpenTelemetry** e **dashboards** não implementados.
- **Reembolso novamente após `ROLLBACK` de `REFUND`** não é suportado (política conservadora descrita acima).
- **Keycloak em modo dev** (`start-dev`, HTTP, banco embutido) — adequado apenas para ambiente local.
- **Moedas** limitadas às de 2 casas decimais.
- O teste de carga é uma linha de base local (Docker Desktop), não um benchmark de produção.
