// Command loadtest drives BET/WIN traffic against one or more running
// instances and reports throughput, latency percentiles, errors, conflicts
// and the outbox lag exposed by /metrics.
//
//	go run ./cmd/loadtest -targets http://localhost:8081,http://localhost:8082,http://localhost:8083 \
//	  -wallets 50 -ops 5000 -concurrency 64 -duplicates 0.1
package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand/v2"
	"net/http"
	"net/url"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

func main() {
	targets := flag.String("targets", "http://localhost:8081,http://localhost:8082,http://localhost:8083", "comma separated API base URLs")
	keycloak := flag.String("keycloak", "http://localhost:8180/realms/jungle", "OIDC issuer URL")
	wallets := flag.Int("wallets", 50, "number of wallets")
	ops := flag.Int("ops", 5000, "number of operations")
	concurrency := flag.Int("concurrency", 64, "parallel clients")
	duplicates := flag.Float64("duplicates", 0.1, "fraction of requests that resend an earlier operation")
	flag.Parse()

	bases := strings.Split(*targets, ",")
	internal := mustToken(*keycloak, "wallet-internal")
	provider := mustToken(*keycloak, "provider-a")
	client := &http.Client{Timeout: 30 * time.Second}

	type wallet struct{ id, player string }
	ws := make([]wallet, *wallets)
	for i := range ws {
		player := uuid.NewString()
		body := fmt.Sprintf(`{"playerId":%q,"initialBalance":{"amount":"100000.00","currency":"BRL"}}`, player)
		var out struct {
			ID string `json:"id"`
		}
		if code := call(client, "POST", bases[i%len(bases)]+"/wallets", internal, body, "", &out); code != 201 {
			fail("open wallet: HTTP %d", code)
		}
		ws[i] = wallet{id: out.ID, player: player}
	}

	type result struct {
		latency time.Duration
		code    int
	}
	results := make([]result, *ops)
	var next atomic.Int64
	var sent sync.Map // index -> body/key for duplicates
	run := fmt.Sprintf("lt-%d", time.Now().UnixNano())
	start := time.Now()
	var wg sync.WaitGroup
	for range *concurrency {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				i := int(next.Add(1)) - 1
				if i >= *ops {
					return
				}
				body, key := "", ""
				if i > 10 && rand.Float64() < *duplicates {
					if v, ok := sent.Load(rand.IntN(i)); ok {
						pair := v.([2]string)
						body, key = pair[0], pair[1]
					}
				}
				if body == "" {
					w := ws[rand.IntN(len(ws))]
					kind, amount := "BET", fmt.Sprintf("%d.%02d", rand.IntN(20)+1, rand.IntN(100))
					if rand.IntN(4) == 0 {
						kind = "WIN"
					}
					ext := fmt.Sprintf("%s-%d", run, i)
					body = fmt.Sprintf(`{"providerId":"provider-a","externalTransactionId":%q,"playerId":%q,"walletId":%q,"roundId":"r-%d","gameId":"load","kind":%q,"money":{"amount":%q,"currency":"BRL"}}`,
						ext, w.player, w.id, i/10, kind, amount)
					key = "provider-a:" + ext
					sent.Store(i, [2]string{body, key})
				}
				t0 := time.Now()
				code := call(client, "POST", bases[i%len(bases)]+"/wagering/transactions", provider, body, key, nil)
				results[i] = result{latency: time.Since(t0), code: code}
			}
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)

	lat := make([]time.Duration, 0, len(results))
	codes := map[int]int{}
	for _, r := range results {
		lat = append(lat, r.latency)
		codes[r.code]++
	}
	slices.Sort(lat)
	pct := func(p float64) time.Duration { return lat[min(len(lat)-1, int(float64(len(lat))*p))] }
	fmt.Printf("targets:     %s\n", *targets)
	fmt.Printf("operations:  %d over %d wallets, concurrency %d, duplicates %.0f%%\n", *ops, *wallets, *concurrency, *duplicates*100)
	fmt.Printf("elapsed:     %s\n", elapsed.Round(time.Millisecond))
	fmt.Printf("throughput:  %.1f req/s\n", float64(*ops)/elapsed.Seconds())
	fmt.Printf("latency:     p50=%s p95=%s p99=%s max=%s\n", pct(0.50), pct(0.95), pct(0.99), lat[len(lat)-1])
	fmt.Printf("status:      %v   (200 processed/replayed, 422 business rejection, 409 conflict, 503 transient)\n", codes)

	time.Sleep(2 * time.Second) // let the relays catch up before sampling the lag
	for _, b := range bases {
		m := scrape(client, b+"/metrics", "outbox_lag_seconds", "outbox_pending_events", "wallet_concurrency_conflicts_total", "wager_duplicates_total")
		fmt.Printf("metrics %s: %v\n", b, m)
	}
	for _, w := range ws {
		var rec struct {
			Consistent bool `json:"consistent"`
		}
		call(client, "POST", bases[0]+"/wallets/"+w.id+"/reconciliation", internal, "", "", &rec)
		if !rec.Consistent {
			fail("wallet %s inconsistent", w.id)
		}
	}
	fmt.Printf("reconciliation: %d wallets consistent\n", len(ws))
}

func call(c *http.Client, method, u, token, body, key string, out any) int {
	req, _ := http.NewRequest(method, u, bytes.NewBufferString(body))
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	resp, err := c.Do(req)
	if err != nil {
		return 0
	}
	defer resp.Body.Close()
	if out != nil {
		_ = json.NewDecoder(resp.Body).Decode(out)
	}
	return resp.StatusCode
}

func mustToken(issuer, client string) string {
	resp, err := http.PostForm(issuer+"/protocol/openid-connect/token", url.Values{
		"grant_type": {"client_credentials"}, "client_id": {client}, "client_secret": {client + "-secret"},
	})
	if err != nil {
		fail("token: %v", err)
	}
	defer resp.Body.Close()
	var body struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil || body.AccessToken == "" {
		fail("token for %s: HTTP %d", client, resp.StatusCode)
	}
	return body.AccessToken
}

func scrape(c *http.Client, u string, names ...string) map[string]string {
	out := map[string]string{}
	resp, err := c.Get(u)
	if err != nil {
		return out
	}
	defer resp.Body.Close()
	sc := bufio.NewScanner(resp.Body)
	for sc.Scan() {
		line := sc.Text()
		for _, n := range names {
			if strings.HasPrefix(line, n+" ") || strings.HasPrefix(line, n+"{") {
				out[strings.Fields(line)[0]] = strings.Fields(line)[1]
			}
		}
	}
	if err := sc.Err(); err != nil {
		out["scrape_error"] = err.Error()
	}
	return out
}

func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
