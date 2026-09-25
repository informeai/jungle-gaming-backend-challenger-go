//go:build integration

// Package integration runs the service against real PostgreSQL, Keycloak and
// LocalStack SQS (docker compose up -d postgres keycloak localstack).
// Every run uses a fresh database and every SQS test its own queues.
package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/fx"

	"github.com/informeai/jungle-gaming-backend-challenger-go/internal/bootstrap"
	"github.com/informeai/jungle-gaming-backend-challenger-go/internal/config"
	"github.com/informeai/jungle-gaming-backend-challenger-go/internal/infra/postgres"
	isqs "github.com/informeai/jungle-gaming-backend-challenger-go/internal/infra/sqs"
	"github.com/informeai/jungle-gaming-backend-challenger-go/internal/observability"
)

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

var (
	pgAdminBase = envOr("IT_PG_ADMIN_URL", "postgres://postgres:postgres@localhost:5432/wallet?sslmode=disable")
	pgAppUser   = envOr("IT_PG_APP_USER", "wallet_app")
	pgAppPass   = envOr("IT_PG_APP_PASSWORD", "wallet_app_local")
	pgRelayUser = envOr("IT_PG_RELAY_USER", "wallet_relay")
	pgRelayPass = envOr("IT_PG_RELAY_PASSWORD", "wallet_relay_local")
	keycloakURL = envOr("KEYCLOAK_URL", "http://localhost:8180")
	awsEndpoint = envOr("IT_AWS_ENDPOINT_URL", "http://localhost:4566")
	oidcIssuer  = keycloakURL + "/realms/jungle"
	testDBName  string
	adminURL    string // owner connection to the test database
	appURL      string // least-privileged runtime connection (money movement)
	relayURL    string // outbox relay connection (outbox delivery columns only)
	relayPool   *pgxpool.Pool
	binPath     string
	adminPool   *pgxpool.Pool
	sqsClient   *sqs.Client
	runID       = fmt.Sprintf("%d%04d", time.Now().Unix()%1_000_000, rand.IntN(10000))
)

func withDB(base, db string) string {
	u, err := url.Parse(base)
	if err != nil {
		panic(err)
	}
	u.Path = "/" + db
	return u.String()
}

func withUser(base, user, pass string) string {
	u, _ := url.Parse(base)
	u.User = url.UserPassword(user, pass)
	return u.String()
}

// createDatabase creates an empty database accessible to the runtime roles.
// The relay role is created when missing (e.g. a Postgres volume initialised
// before the role existed), mirroring deploy/postgres/init.sql.
func createDatabase(ctx context.Context, name string) error {
	conn, err := pgx.Connect(ctx, pgAdminBase)
	if err != nil {
		return fmt.Errorf("connect admin (is `docker compose up -d postgres` running?): %w", err)
	}
	defer conn.Close(ctx)
	var exists bool
	if err := conn.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = $1)`, pgRelayUser).Scan(&exists); err != nil {
		return err
	}
	if !exists {
		if _, err := conn.Exec(ctx, fmt.Sprintf("CREATE ROLE %s LOGIN PASSWORD '%s'", pgx.Identifier{pgRelayUser}.Sanitize(), pgRelayPass)); err != nil {
			return err
		}
	}
	if _, err := conn.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{name}.Sanitize()); err != nil {
		return err
	}
	_, err = conn.Exec(ctx, fmt.Sprintf("GRANT CONNECT ON DATABASE %s TO %s, %s", pgx.Identifier{name}.Sanitize(),
		pgx.Identifier{pgAppUser}.Sanitize(), pgx.Identifier{pgRelayUser}.Sanitize()))
	return err
}

func dropDatabase(name string) {
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, pgAdminBase)
	if err != nil {
		return
	}
	defer conn.Close(ctx)
	_, _ = conn.Exec(ctx, "DROP DATABASE IF EXISTS "+pgx.Identifier{name}.Sanitize()+" WITH (FORCE)")
}

func TestMain(m *testing.M) {
	os.Exit(run(m))
}

func run(m *testing.M) int {
	ctx := context.Background()
	testDBName = "it_" + runID
	if err := createDatabase(ctx, testDBName); err != nil {
		fmt.Fprintln(os.Stderr, "integration setup:", err)
		return 1
	}
	if os.Getenv("IT_KEEP_DB") == "" {
		defer dropDatabase(testDBName)
	}
	adminURL = withDB(pgAdminBase, testDBName)
	appURL = withUser(adminURL, pgAppUser, pgAppPass)
	relayURL = withUser(adminURL, pgRelayUser, pgRelayPass)
	if err := postgres.MigrateUp(adminURL); err != nil {
		fmt.Fprintln(os.Stderr, "migrate:", err)
		return 1
	}
	var err error
	if adminPool, err = pgxpool.New(ctx, adminURL); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	defer adminPool.Close()
	if relayPool, err = pgxpool.New(ctx, relayURL); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	defer relayPool.Close()
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion("us-east-1"),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("provider-gateway", "provider-gateway-local", "")))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	sqsClient = sqs.NewFromConfig(awsCfg, func(o *sqs.Options) { o.BaseEndpoint = aws.String(awsEndpoint) })

	dir, err := os.MkdirTemp("", "wallet-it")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	defer os.RemoveAll(dir)
	binPath = filepath.Join(dir, "wallet-service")
	build := exec.Command("go", "build", "-race", "-o", binPath, "../../cmd/wallet-service")
	build.Stdout, build.Stderr = os.Stderr, os.Stderr
	if err := build.Run(); err != nil {
		fmt.Fprintln(os.Stderr, "build:", err)
		return 1
	}
	return m.Run()
}

// ---------------------------------------------------------------- queues ---

type queueSet struct {
	Ingress, DLQ, Events string
	IngressURL, DLQURL   string
	EventsURL            string
}

// createQueues provisions isolated FIFO queues (with redrive) for one test.
func createQueues(t *testing.T) queueSet {
	t.Helper()
	ctx := context.Background()
	prefix := fmt.Sprintf("it%s-%d", runID, rand.IntN(1_000_000))
	q := queueSet{Ingress: prefix + "-wager.fifo", DLQ: prefix + "-wager-dlq.fifo", Events: prefix + "-events.fifo"}
	fifo := map[string]string{"FifoQueue": "true", "ContentBasedDeduplication": "false"}
	dlq, err := sqsClient.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String(q.DLQ), Attributes: fifo})
	if err != nil {
		t.Fatalf("create dlq (is localstack running?): %v", err)
	}
	q.DLQURL = aws.ToString(dlq.QueueUrl)
	attrs, _ := sqsClient.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{QueueUrl: dlq.QueueUrl, AttributeNames: []sqstypesAttr{"QueueArn"}})
	redrive, _ := json.Marshal(map[string]string{"deadLetterTargetArn": attrs.Attributes["QueueArn"], "maxReceiveCount": "10"})
	in, err := sqsClient.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String(q.Ingress), Attributes: map[string]string{
		"FifoQueue": "true", "ContentBasedDeduplication": "false", "VisibilityTimeout": "5", "RedrivePolicy": string(redrive),
	}})
	if err != nil {
		t.Fatal(err)
	}
	q.IngressURL = aws.ToString(in.QueueUrl)
	ev, err := sqsClient.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String(q.Events), Attributes: fifo})
	if err != nil {
		t.Fatal(err)
	}
	q.EventsURL = aws.ToString(ev.QueueUrl)
	t.Cleanup(func() {
		for _, u := range []string{q.IngressURL, q.DLQURL, q.EventsURL} {
			_, _ = sqsClient.DeleteQueue(context.Background(), &sqs.DeleteQueueInput{QueueUrl: aws.String(u)})
		}
	})
	return q
}

// --------------------------------------------------------------- config ---

func baseConfig(q queueSet) config.Config {
	return config.Config{
		InstanceID: "it-" + uuid.NewString()[:8],
		LogLevel:   envOr("IT_LOG_LEVEL", "error"),
		HTTP: config.HTTP{APIEnabled: true, Addr: "127.0.0.1:0", ReadTimeout: 10 * time.Second, WriteTimeout: 15 * time.Second,
			RequestTimeout: 10 * time.Second, ShutdownTimeout: 10 * time.Second},
		Database: config.Database{URL: appURL, MaxConns: 30, ConnectTimeout: 3 * time.Second},
		AWS:      config.AWS{Region: "us-east-1", EndpointURL: awsEndpoint},
		SQS: config.SQS{ConsumerEnabled: true, ConsumerName: "wager-transactions-consumer",
			IngressQueue: q.Ingress, IngressDLQ: q.DLQ, EventsQueue: q.Events, MaxMessages: 10,
			WaitTime: time.Second, VisibilityTimeout: 5 * time.Second, MaxReceives: 3, Concurrency: 4,
			HandlerTimeout: 4 * time.Second, RetryBaseDelay: time.Second, RetryMaxDelay: 2 * time.Second,
			AllowedProviders: []string{"provider-a", "provider-b"}},
		Outbox: config.Outbox{Enabled: true, DatabaseURL: relayURL, PollInterval: 100 * time.Millisecond, BatchSize: 50,
			Lease: 5 * time.Second, BaseBackoff: 200 * time.Millisecond, MaxBackoff: 2 * time.Second},
		Pending: config.Pending{Enabled: true, PollInterval: 100 * time.Millisecond, BatchSize: 50,
			MaxAttempts: 12, BaseDelay: 200 * time.Millisecond, MaxDelay: time.Second, TTL: time.Minute},
		OIDC: config.OIDC{Issuer: oidcIssuer, Audience: "wallet-api", ProviderClaim: "provider_id",
			ProviderRole: "wagering-provider", InternalRole: "wallet-internal"},
		Breaker: config.Breaker{FailureThreshold: 5, OpenTimeout: 5 * time.Second},
	}
}

// configEnv renders a config as the environment of a child process.
func configEnv(c config.Config) []string {
	b := func(v bool) string { return fmt.Sprint(v) }
	return []string{
		"INSTANCE_ID=" + c.InstanceID, "LOG_LEVEL=" + c.LogLevel, "HTTP_ADDR=" + c.HTTP.Addr,
		"FAULT_INJECTION=" + c.FaultInjection,
		"DATABASE_URL=" + c.Database.URL, "DATABASE_MAX_CONNS=" + fmt.Sprint(c.Database.MaxConns),
		"AWS_REGION=us-east-1", "AWS_ENDPOINT_URL=" + c.AWS.EndpointURL,
		"AWS_ACCESS_KEY_ID=wallet-service", "AWS_SECRET_ACCESS_KEY=wallet-service-local",
		"SQS_CONSUMER_ENABLED=" + b(c.SQS.ConsumerEnabled), "SQS_INGRESS_QUEUE=" + c.SQS.IngressQueue,
		"SQS_INGRESS_DLQ=" + c.SQS.IngressDLQ, "SQS_EVENTS_QUEUE=" + c.SQS.EventsQueue,
		"SQS_WAIT_TIME=" + c.SQS.WaitTime.String(), "SQS_VISIBILITY_TIMEOUT=" + c.SQS.VisibilityTimeout.String(),
		"SQS_HANDLER_TIMEOUT=" + c.SQS.HandlerTimeout.String(), "SQS_MAX_RECEIVES=" + fmt.Sprint(c.SQS.MaxReceives),
		"API_ENABLED=" + b(c.HTTP.APIEnabled), "OUTBOX_DATABASE_URL=" + c.Outbox.DatabaseURL,
		"OUTBOX_ENABLED=" + b(c.Outbox.Enabled), "OUTBOX_POLL_INTERVAL=" + c.Outbox.PollInterval.String(),
		"OUTBOX_LEASE=" + c.Outbox.Lease.String(),
		"PENDING_WORKER_ENABLED=" + b(c.Pending.Enabled), "PENDING_POLL_INTERVAL=" + c.Pending.PollInterval.String(),
		"REFERENCE_BASE_DELAY=" + c.Pending.BaseDelay.String(), "REFERENCE_MAX_DELAY=" + c.Pending.MaxDelay.String(),
		"REFERENCE_MAX_ATTEMPTS=" + fmt.Sprint(c.Pending.MaxAttempts), "REFERENCE_TTL=" + c.Pending.TTL.String(),
		"OIDC_ISSUER=" + c.OIDC.Issuer, "OIDC_AUDIENCE=" + c.OIDC.Audience,
		"BREAKER_FAILURE_THRESHOLD=" + fmt.Sprint(c.Breaker.FailureThreshold), "BREAKER_OPEN_TIMEOUT=" + c.Breaker.OpenTimeout.String(),
		"PATH=" + os.Getenv("PATH"), "HOME=" + os.Getenv("HOME"),
	}
}

// ------------------------------------------------------------- instances ---

type instance struct {
	Base     string
	App      *fx.App
	Metrics  *observability.Metrics
	Consumer *isqs.Consumer
	Outbox   *bootstrap.OutboxLoop
	Pending  *bootstrap.PendingLoop
	Pool     *pgxpool.Pool
	stopped  bool
}

// startApp runs the Fx application in-process with the components enabled in
// cfg (own pools and workers). Fields of disabled components stay nil.
func startApp(t *testing.T, cfg config.Config) *instance {
	t.Helper()
	inst := &instance{}
	var srv *bootstrap.Server
	targets := []any{&srv, &inst.Metrics}
	if cfg.NeedsAppDatabase() {
		targets = append(targets, &inst.Pool)
	}
	if cfg.SQS.ConsumerEnabled {
		targets = append(targets, &inst.Consumer)
	}
	if cfg.Outbox.Enabled {
		targets = append(targets, &inst.Outbox)
	}
	if cfg.Pending.Enabled {
		targets = append(targets, &inst.Pending)
	}
	app := fx.New(bootstrap.Options(cfg), fx.NopLogger, fx.Populate(targets...))
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := app.Start(ctx); err != nil {
		t.Fatalf("start app: %v", err)
	}
	inst.App, inst.Base = app, "http://"+srv.Addr
	t.Cleanup(func() { inst.Stop(t) })
	return inst
}

func (i *instance) Stop(t *testing.T) {
	if i.stopped {
		return
	}
	i.stopped = true
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := i.App.Stop(ctx); err != nil {
		t.Errorf("stop app: %v", err)
	}
}

// process is an independent OS process running the service binary.
type process struct {
	Base     string
	cmd      *exec.Cmd
	out      *lockedBuffer
	exited   chan struct{} // closed once Wait returns
	exitCode int           // valid after exited is closed
}

type lockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *lockedBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *lockedBuffer) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

func freePort(t *testing.T) int {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

// startProcess launches the binary and waits until it is ready.
func startProcess(t *testing.T, cfg config.Config) *process {
	t.Helper()
	return launch(t, cfg, true)
}

// launch starts the binary; with waitReady it blocks until /health/ready.
func launch(t *testing.T, cfg config.Config, waitReady bool) *process {
	t.Helper()
	port := freePort(t)
	cfg.HTTP.Addr = fmt.Sprintf("127.0.0.1:%d", port)
	cfg.LogLevel = "info"
	p := &process{Base: fmt.Sprintf("http://127.0.0.1:%d", port), out: &lockedBuffer{}, exited: make(chan struct{})}
	p.cmd = exec.Command(binPath, "serve")
	p.cmd.Env = configEnv(cfg)
	p.cmd.Stdout, p.cmd.Stderr = p.out, p.out
	if err := p.cmd.Start(); err != nil {
		t.Fatal(err)
	}
	go func() {
		_ = p.cmd.Wait()
		p.exitCode = p.cmd.ProcessState.ExitCode()
		close(p.exited)
	}()
	t.Cleanup(func() {
		p.Kill()
		if strings.Contains(p.out.String(), "WARNING: DATA RACE") {
			t.Errorf("data race detected in process %s", cfg.InstanceID)
		}
		if t.Failed() {
			t.Logf("process %s output:\n%s", cfg.InstanceID, tail(p.out.String(), 4000))
		}
	})
	if !waitReady {
		return p
	}
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case <-p.exited:
			t.Fatalf("process exited during startup (code %d)\n%s", p.exitCode, p.out.String())
		default:
		}
		resp, err := http.Get(p.Base + "/health/ready")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return p
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("process not ready:\n%s", p.out.String())
	return nil
}

func tail(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[len(s)-n:]
}

// Kill sends SIGKILL (abrupt termination, no shutdown hooks).
func (p *process) Kill() {
	select {
	case <-p.exited:
		return
	default:
	}
	_ = p.cmd.Process.Kill()
	select {
	case <-p.exited:
	case <-time.After(10 * time.Second):
	}
}

// WaitExit waits for the process to exit on its own.
func (p *process) WaitExit(t *testing.T, timeout time.Duration) int {
	t.Helper()
	select {
	case <-p.exited:
		return p.exitCode
	case <-time.After(timeout):
		t.Fatalf("process did not exit:\n%s", tail(p.out.String(), 3000))
	}
	return -1
}

// --------------------------------------------------------------- tokens ---

var (
	tokenMu    sync.Mutex
	tokenCache = map[string]cachedToken{}
)

type cachedToken struct {
	value string
	at    time.Time
}

func fetchToken(t *testing.T, client string) string {
	t.Helper()
	resp, err := http.PostForm(oidcIssuer+"/protocol/openid-connect/token", url.Values{
		"grant_type": {"client_credentials"}, "client_id": {client}, "client_secret": {client + "-secret"},
	})
	if err != nil {
		t.Fatalf("keycloak token (is keycloak running?): %v", err)
	}
	defer resp.Body.Close()
	var body struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil || body.AccessToken == "" {
		t.Fatalf("token for %s: status %d %v", client, resp.StatusCode, err)
	}
	return body.AccessToken
}

func token(t *testing.T, client string) string {
	t.Helper()
	tokenMu.Lock()
	defer tokenMu.Unlock()
	if c, ok := tokenCache[client]; ok && time.Since(c.at) < 3*time.Minute {
		return c.value
	}
	v := fetchToken(t, client)
	tokenCache[client] = cachedToken{value: v, at: time.Now()}
	return v
}

// ----------------------------------------------------------------- http ---

var httpClient = &http.Client{Timeout: 20 * time.Second}

type response struct {
	Status int
	Body   map[string]any
	Raw    string
	Header http.Header
}

func (r response) str(path ...string) string {
	var cur any = r.Body
	for _, p := range path {
		m, ok := cur.(map[string]any)
		if !ok {
			return ""
		}
		cur = m[p]
	}
	s, _ := cur.(string)
	return s
}

func do(t *testing.T, method, u, tok string, body any, headers map[string]string) response {
	t.Helper()
	var rd io.Reader
	if body != nil {
		switch b := body.(type) {
		case string:
			rd = strings.NewReader(b)
		default:
			raw, _ := json.Marshal(b)
			rd = bytes.NewReader(raw)
		}
	}
	req, _ := http.NewRequest(method, u, rd)
	req.Header.Set("Content-Type", "application/json")
	if tok != "" {
		req.Header.Set("Authorization", "Bearer "+tok)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, u, err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	r := response{Status: resp.StatusCode, Raw: string(raw), Header: resp.Header}
	_ = json.Unmarshal(raw, &r.Body)
	return r
}

type wagerOp struct {
	Provider, External, Player, Wallet, Round, Kind, Amount, Reference string
}

func (o wagerOp) payload() map[string]any {
	p := map[string]any{
		"providerId": o.Provider, "externalTransactionId": o.External, "playerId": o.Player, "walletId": o.Wallet,
		"roundId": o.Round, "gameId": "fortune-chimp", "kind": o.Kind,
		"money": map[string]string{"amount": o.Amount, "currency": "BRL"},
	}
	if o.Reference != "" {
		p["referenceExternalTransactionId"] = o.Reference
	}
	return p
}

func (o wagerOp) key() string { return o.Provider + ":" + o.External }

func submit(t *testing.T, base string, o wagerOp) response {
	t.Helper()
	return do(t, http.MethodPost, base+"/wagering/transactions", token(t, o.Provider), o.payload(), map[string]string{"Idempotency-Key": o.key()})
}

type walletRef struct{ ID, Player string }

func openWallet(t *testing.T, base, amount string) walletRef {
	t.Helper()
	player := uuid.NewString()
	r := do(t, http.MethodPost, base+"/wallets", token(t, "wallet-internal"),
		map[string]any{"playerId": player, "initialBalance": map[string]string{"amount": amount, "currency": "BRL"}}, nil)
	if r.Status != http.StatusCreated {
		t.Fatalf("open wallet: %d %s", r.Status, r.Raw)
	}
	return walletRef{ID: r.str("id"), Player: player}
}

func (w walletRef) op(ext, kind, amount, ref string) wagerOp {
	return wagerOp{Provider: "provider-a", External: ext, Player: w.Player, Wallet: w.ID, Round: "round-1", Kind: kind, Amount: amount, Reference: ref}
}

func uniq(prefix string) string { return prefix + "-" + uuid.NewString()[:12] }

// ------------------------------------------------------------ assertions ---

type walletState struct {
	Balance       int64
	Version       int64
	LedgerSum     int64
	LedgerEntries int64
	Debits        int64
}

func stateOf(t *testing.T, walletID string) walletState {
	t.Helper()
	var s walletState
	err := adminPool.QueryRow(context.Background(), `
		SELECT w.balance_minor, w.version,
		       COALESCE(SUM(CASE WHEN l.direction = 'CREDIT' THEN l.amount_minor ELSE -l.amount_minor END), 0)::bigint,
		       COUNT(l.id), COUNT(l.id) FILTER (WHERE l.direction = 'DEBIT')
		FROM wallets w LEFT JOIN wallet_ledger_entries l ON l.wallet_id = w.id
		WHERE w.id = $1 GROUP BY w.id`, walletID).Scan(&s.Balance, &s.Version, &s.LedgerSum, &s.LedgerEntries, &s.Debits)
	if err != nil {
		t.Fatalf("wallet state: %v", err)
	}
	if s.Balance != s.LedgerSum {
		t.Fatalf("stored balance %d != ledger sum %d", s.Balance, s.LedgerSum)
	}
	return s
}

func assertReconciled(t *testing.T, base, walletID string) {
	t.Helper()
	r := do(t, http.MethodPost, base+"/wallets/"+walletID+"/reconciliation", token(t, "wallet-internal"), nil, nil)
	if r.Status != http.StatusOK || r.Body["consistent"] != true || r.str("difference", "amount") != "0.00" {
		t.Fatalf("reconciliation: %d %s", r.Status, r.Raw)
	}
}

func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func txStatus(t *testing.T, provider, external string) (status, failure string) {
	t.Helper()
	var f *string
	err := adminPool.QueryRow(context.Background(), `SELECT status, failure_code FROM wager_transactions
		WHERE provider_id = $1 AND external_transaction_id = $2`, provider, external).Scan(&status, &f)
	if err == pgx.ErrNoRows {
		return "", ""
	}
	if err != nil {
		t.Fatal(err)
	}
	if f != nil {
		failure = *f
	}
	return status, failure
}

// ------------------------------------------------------------------ sqs ---

func sendMessage(t *testing.T, queueURL, messageID string, o wagerOp, dedup string) {
	t.Helper()
	body, _ := json.Marshal(map[string]any{
		"messageId": messageID, "type": "WagerTransactionRequested", "occurredAt": time.Now().UTC().Format(time.RFC3339Nano),
		"data": func() map[string]any { p := o.payload(); p["idempotencyKey"] = o.key(); return p }(),
	})
	sendRaw(t, queueURL, string(body), o.Wallet, dedup)
}

func sendRaw(t *testing.T, queueURL, body, group, dedup string) {
	t.Helper()
	_, err := sqsClient.SendMessage(context.Background(), &sqs.SendMessageInput{
		QueueUrl: aws.String(queueURL), MessageBody: aws.String(body),
		MessageGroupId: aws.String(group), MessageDeduplicationId: aws.String(dedup),
	})
	if err != nil {
		t.Fatalf("send message: %v", err)
	}
}

func queueDepth(t *testing.T, queueURL string) (visible, inflight int) {
	t.Helper()
	out, err := sqsClient.GetQueueAttributes(context.Background(), &sqs.GetQueueAttributesInput{
		QueueUrl: aws.String(queueURL), AttributeNames: []sqstypesAttr{"ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"},
	})
	if err != nil {
		t.Fatal(err)
	}
	fmt.Sscan(out.Attributes["ApproximateNumberOfMessages"], &visible)
	fmt.Sscan(out.Attributes["ApproximateNumberOfMessagesNotVisible"], &inflight)
	return visible, inflight
}

// receiveAll drains messages from a queue (used on DLQ and events queues).
func receiveAll(t *testing.T, queueURL string, wait time.Duration) []sqsMessage {
	t.Helper()
	var out []sqsMessage
	deadline := time.Now().Add(wait)
	for time.Now().Before(deadline) {
		res, err := sqsClient.ReceiveMessage(context.Background(), &sqs.ReceiveMessageInput{
			QueueUrl: aws.String(queueURL), MaxNumberOfMessages: 10, WaitTimeSeconds: 1, VisibilityTimeout: 60,
			MessageAttributeNames: []string{"All"},
		})
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range res.Messages {
			attrs := map[string]string{}
			for k, v := range m.MessageAttributes {
				attrs[k] = aws.ToString(v.StringValue)
			}
			out = append(out, sqsMessage{Body: aws.ToString(m.Body), Attributes: attrs})
			_, _ = sqsClient.DeleteMessage(context.Background(), &sqs.DeleteMessageInput{QueueUrl: aws.String(queueURL), ReceiptHandle: m.ReceiptHandle})
		}
	}
	return out
}

type sqsMessage struct {
	Body       string
	Attributes map[string]string
}

func parallel(n int, fn func(i int)) {
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			fn(i)
		}()
	}
	close(start)
	wg.Wait()
}

type sqstypesAttr = types.QueueAttributeName

type cfgT = config.Config

// httpConfig is a config with the SQS consumer disabled (isolated queues are
// still created so readiness and the outbox relay have real targets).
func httpConfig(t *testing.T) config.Config {
	t.Helper()
	c := baseConfig(createQueues(t))
	c.SQS.ConsumerEnabled = false
	return c
}

func uniqUUID() string { return uuid.NewString() }

func uniqf(prefix string, a, b int) string {
	return fmt.Sprintf("%s-%d-%d-%s", prefix, a, b, uuid.NewString()[:8])
}
