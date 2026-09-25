package bootstrap

import (
	"testing"

	"go.uber.org/fx"

	"github.com/informeai/jungle-gaming-backend-challenger-go/internal/config"
)

// TestGraphIsValid checks, for the all-in-one process and for every
// single-component process, that each constructor dependency can be
// satisfied, without running constructors or touching infrastructure. The
// start/stop cycle is exercised by the integration tests.
func TestGraphIsValid(t *testing.T) {
	components := map[string]map[string]string{
		"all-in-one": {},
		"api":        {"SQS_CONSUMER_ENABLED": "false", "OUTBOX_ENABLED": "false", "PENDING_WORKER_ENABLED": "false"},
		"consumer":   {"API_ENABLED": "false", "OUTBOX_ENABLED": "false", "PENDING_WORKER_ENABLED": "false", "OIDC_ISSUER": ""},
		"pending":    {"API_ENABLED": "false", "SQS_CONSUMER_ENABLED": "false", "OUTBOX_ENABLED": "false", "OIDC_ISSUER": ""},
		// The relay runs without the money-moving database credentials.
		"relay": {"API_ENABLED": "false", "SQS_CONSUMER_ENABLED": "false", "PENDING_WORKER_ENABLED": "false", "OIDC_ISSUER": "", "DATABASE_URL": ""},
	}
	for name, env := range components {
		t.Run(name, func(t *testing.T) {
			t.Setenv("DATABASE_URL", "postgres://u:p@localhost:1/db")
			t.Setenv("OUTBOX_DATABASE_URL", "postgres://relay:p@localhost:1/db")
			t.Setenv("OIDC_ISSUER", "http://localhost/realms/jungle")
			for k, v := range env {
				t.Setenv(k, v)
			}
			cfg, err := config.Load()
			if err != nil {
				t.Fatal(err)
			}
			if err := fx.ValidateApp(Options(cfg)); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestConfigValidation(t *testing.T) {
	invalid := map[string]map[string]string{
		"missing everything":            {"DATABASE_URL": "", "OIDC_ISSUER": "", "SQS_MAX_MESSAGES": "50"},
		"handler timeout >= visibility": {"SQS_HANDLER_TIMEOUT": "1m", "SQS_VISIBILITY_TIMEOUT": "30s"},
		"api without issuer":            {"OIDC_ISSUER": ""},
		"relay without its db url":      {"OUTBOX_DATABASE_URL": ""},
		"money component without db":    {"DATABASE_URL": ""},
		"no component enabled": {"API_ENABLED": "false", "SQS_CONSUMER_ENABLED": "false",
			"OUTBOX_ENABLED": "false", "PENDING_WORKER_ENABLED": "false"},
	}
	for name, env := range invalid {
		t.Run(name, func(t *testing.T) {
			t.Setenv("DATABASE_URL", "postgres://x")
			t.Setenv("OUTBOX_DATABASE_URL", "postgres://relay")
			t.Setenv("OIDC_ISSUER", "http://x")
			for k, v := range env {
				t.Setenv(k, v)
			}
			if _, err := config.Load(); err == nil {
				t.Fatal("configuration must be rejected at startup")
			}
		})
	}
}
