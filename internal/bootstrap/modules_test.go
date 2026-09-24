package bootstrap

import (
	"testing"

	"go.uber.org/fx"

	"github.com/informeai/jungle-gaming-backend-challenger-go/internal/config"
)

// TestGraphIsValid checks that every constructor dependency can be satisfied,
// without running constructors or touching infrastructure. The full start and
// stop cycle is exercised by the integration tests.
func TestGraphIsValid(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://u:p@localhost:1/db")
	t.Setenv("OIDC_ISSUER", "http://localhost/realms/jungle")
	cfg, err := config.Load()
	if err != nil {
		t.Fatal(err)
	}
	if err := fx.ValidateApp(Options(cfg)); err != nil {
		t.Fatal(err)
	}
}

func TestConfigValidation(t *testing.T) {
	t.Setenv("DATABASE_URL", "")
	t.Setenv("OIDC_ISSUER", "")
	t.Setenv("SQS_MAX_MESSAGES", "50")
	if _, err := config.Load(); err == nil {
		t.Fatal("invalid configuration must fail at startup")
	}
	t.Setenv("DATABASE_URL", "postgres://x")
	t.Setenv("OIDC_ISSUER", "http://x")
	t.Setenv("SQS_MAX_MESSAGES", "10")
	t.Setenv("SQS_HANDLER_TIMEOUT", "1m")
	t.Setenv("SQS_VISIBILITY_TIMEOUT", "30s")
	if _, err := config.Load(); err == nil {
		t.Fatal("handler timeout >= visibility timeout must be rejected")
	}
}
