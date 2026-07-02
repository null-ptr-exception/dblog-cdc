package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/godror/godror"

	"github.com/null-ptr-exception/dblog-cdc/internal/chunk"
	"github.com/null-ptr-exception/dblog-cdc/internal/config"
	"github.com/null-ptr-exception/dblog-cdc/internal/olr"
	"github.com/null-ptr-exception/dblog-cdc/internal/progress"
	"github.com/null-ptr-exception/dblog-cdc/internal/replicator"
	"github.com/null-ptr-exception/dblog-cdc/internal/transform"
	"github.com/null-ptr-exception/dblog-cdc/internal/writer"
)

func main() {
	configPath := flag.String("config", "config.yaml", "path to config file")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		slog.Error("load config", "error", err)
		os.Exit(1)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	oracleDB, err := sql.Open("godror", cfg.Source.DSN)
	if err != nil {
		slog.Error("connect oracle", "error", err)
		os.Exit(1)
	}
	defer oracleDB.Close()

	ybPool, err := pgxpool.New(ctx, cfg.Target.DSN)
	if err != nil {
		slog.Error("connect yugabytedb", "error", err)
		os.Exit(1)
	}
	defer ybPool.Close()

	pgStore := progress.NewPgStore(ybPool, cfg.Progress.Table)
	if err := pgStore.EnsureTable(ctx); err != nil {
		slog.Error("ensure progress table", "error", err)
		os.Exit(1)
	}

	tableNames := make([]string, len(cfg.Tables))
	pkColumns := make(map[string]string, len(cfg.Tables))
	for i, t := range cfg.Tables {
		tableNames[i] = t.Name
		pkColumns[t.Name] = t.PKColumn
	}

	typeMap, err := transform.LoadTypeMap(ctx, oracleDB, tableNames)
	if err != nil {
		slog.Error("load type map", "error", err)
		os.Exit(1)
	}
	transformer := transform.New(typeMap)

	for _, tbl := range cfg.Tables {
		slog.Info("starting replication", "table", tbl.Name)

		cdcClient := olr.NewClient(cfg.CDC.Host, cfg.CDC.Port, "", tableNames, pkColumns)
		querier := chunk.NewOracleQuerier(oracleDB, tbl.PKColumn)
		ybWriter := writer.NewPgWriter(ybPool, tbl.PKColumn)

		r := replicator.New(cdcClient, querier, ybWriter, pgStore, tbl)
		r.SetTransformer(transformer)
		if err := r.Run(ctx); err != nil {
			slog.Error("replication failed", "table", tbl.Name, "error", err)
			os.Exit(1)
		}
	}

	fmt.Println("replication complete")
}
