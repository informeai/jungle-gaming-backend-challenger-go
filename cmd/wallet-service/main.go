// Command wallet-service runs the wallet API, the SQS consumer and the
// background workers ("serve", default) or manages the schema ("migrate").
//
//	wallet-service serve
//	wallet-service migrate up
//	wallet-service migrate down [steps]   (all when omitted)
//	wallet-service migrate version
package main

import (
	"fmt"
	"os"
	"strconv"

	"go.uber.org/fx"

	"github.com/informeai/jungle-gaming-backend-challenger-go/internal/bootstrap"
	"github.com/informeai/jungle-gaming-backend-challenger-go/internal/config"
	"github.com/informeai/jungle-gaming-backend-challenger-go/internal/infra/postgres"
)

func main() {
	args := os.Args[1:]
	if len(args) == 0 || args[0] == "serve" {
		serve()
		return
	}
	if args[0] == "migrate" {
		if err := migrate(args[1:]); err != nil {
			fmt.Fprintln(os.Stderr, "migrate:", err)
			os.Exit(1)
		}
		return
	}
	fmt.Fprintf(os.Stderr, "unknown command %q (use serve | migrate up|down [n]|version)\n", args[0])
	os.Exit(2)
}

func serve() {
	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintln(os.Stderr, "invalid configuration:", err)
		os.Exit(1)
	}
	// Run blocks until SIGINT/SIGTERM, then runs the OnStop hooks.
	fx.New(bootstrap.Options(cfg)).Run()
}

func migrate(args []string) error {
	url := os.Getenv("MIGRATIONS_DATABASE_URL")
	if url == "" {
		url = os.Getenv("DATABASE_URL")
	}
	if url == "" {
		return fmt.Errorf("MIGRATIONS_DATABASE_URL (or DATABASE_URL) is required")
	}
	if len(args) == 0 {
		return fmt.Errorf("expected up | down [steps] | version")
	}
	switch args[0] {
	case "up":
		if err := postgres.MigrateUp(url); err != nil {
			return err
		}
	case "down":
		steps := 0
		if len(args) > 1 {
			n, err := strconv.Atoi(args[1])
			if err != nil || n < 1 {
				return fmt.Errorf("invalid steps %q", args[1])
			}
			steps = n
		}
		if err := postgres.MigrateDown(url, steps); err != nil {
			return err
		}
	case "version":
	default:
		return fmt.Errorf("unknown migrate command %q", args[0])
	}
	v, err := postgres.MigrationVersion(url)
	if err != nil {
		return err
	}
	fmt.Println("schema version:", v)
	return nil
}
