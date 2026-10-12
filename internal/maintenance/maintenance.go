// Package maintenance holds the daily Postgres housekeeping run by the
// scheduler (Cronicle locally, Airflow/MWAA in production): claims retention
// and VACUUM. autovacuum keeps running; these complement it.
package maintenance

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"coingecko-etl/internal/storage/pg"
	"coingecko-etl/internal/xtime"
)

// RetentionOptions configures one Retention run.
type RetentionOptions struct {
	Days        int           // cfg.ClaimsRetentionDays; must be >= 2 so today-1..today+3 (EnsureClaimPartitions) is never touched
	LockTimeout time.Duration // per DROP attempt
	Attempts    int           // DROP attempts per partition on lock timeout
	RetryDelay  time.Duration // pause between attempts
}

// DefaultRetentionOptions keeps days of claims and bounds each DROP's lock
// wait to 3 s, five attempts 2 s apart.
func DefaultRetentionOptions(days int) RetentionOptions {
	return RetentionOptions{Days: days, LockTimeout: 3 * time.Second, Attempts: 5, RetryDelay: 2 * time.Second}
}

// RetentionResult is what one Retention run removed.
type RetentionResult struct {
	Cutoff            time.Time // UTC midnight of (UTC date of now) - Days
	DroppedPartitions []string  // qualified names, in drop order (oldest first)
	BatchesDeleted    int64
}

// deleteWrittenBatchesSQL needs no index on written_at: the table holds about
// one row per transformer batch and is scanned once a day.
const deleteWrittenBatchesSQL = `DELETE FROM pipeline.consumer_batches WHERE status = 'written' AND written_at < $1`

// Retention removes the dedup history the pipeline can no longer need.
// Partitions of pipeline.dedup_claims whose upper bound is <= Cutoff are
// dropped, oldest first. Then rows of pipeline.consumer_batches with status
// 'written' and written_at < Cutoff are deleted. 'planned' rows are never
// touched (re-planning and manual repair need them), and neither are the
// raw.api_calls partitions or the export ledger (the export_batch_id foreign
// key forbids deleting ledger rows before raw ones). It is idempotent and
// returns the partial result with an error.
//
// Example: now 2026-10-12T11:00Z, Days 8: Cutoff is 2026-10-04, so claims of
// event dates up to 2026-10-03 go and 2026-10-04 stays.
func Retention(ctx context.Context, pool *pgxpool.Pool, now time.Time, opt RetentionOptions, log *slog.Logger) (RetentionResult, error) {
	res := RetentionResult{Cutoff: retentionCutoff(now, opt.Days)}
	switch {
	case opt.Days < 2:
		return res, fmt.Errorf("maintenance: retention of %d days is below 2: it would drop partitions EnsureClaimPartitions keeps", opt.Days)
	case opt.Attempts < 1:
		return res, fmt.Errorf("maintenance: %d drop attempts, want >= 1", opt.Attempts)
	}

	parts, skipped, err := pg.ListClaimPartitions(ctx, pool)
	if err != nil {
		return res, err
	}
	for _, name := range skipped {
		log.Warn("claims partition bound not understood, kept", "partition", name)
	}
	for _, p := range parts {
		if p.To.After(res.Cutoff) {
			break // sorted by To
		}
		if err := dropWithRetry(ctx, pool, p, opt, log); err != nil {
			return res, err
		}
		res.DroppedPartitions = append(res.DroppedPartitions, p.QualifiedName())
		log.Info("dropped claims partition", "partition", p.QualifiedName(),
			"from", xtime.FormatDate(p.From), "to", xtime.FormatDate(p.To))
	}

	tag, err := pool.Exec(ctx, deleteWrittenBatchesSQL, res.Cutoff)
	if err != nil {
		return res, fmt.Errorf("maintenance: delete written consumer batches: %w", err)
	}
	res.BatchesDeleted = tag.RowsAffected()
	log.Info("retention finished", "cutoff", xtime.FormatDate(res.Cutoff), "days", opt.Days,
		"partitions_dropped", len(res.DroppedPartitions), "batches_deleted", res.BatchesDeleted)
	return res, nil
}

// retentionCutoff is UTC midnight of now's UTC date minus days.
func retentionCutoff(now time.Time, days int) time.Time {
	u := now.UTC()
	return time.Date(u.Year(), u.Month(), u.Day(), 0, 0, 0, 0, time.UTC).AddDate(0, 0, -days)
}

// dropWithRetry drops p, retrying a lock timeout up to opt.Attempts times in
// all. Each attempt keeps claim inserts waiting at most opt.LockTimeout.
func dropWithRetry(ctx context.Context, pool *pgxpool.Pool, p pg.ClaimPartition, opt RetentionOptions, log *slog.Logger) error {
	for attempt := 1; ; attempt++ {
		err := pg.DropClaimPartition(ctx, pool, p, opt.LockTimeout)
		if err == nil {
			return nil
		}
		if !pg.IsLockTimeout(err) {
			return err
		}
		if attempt == opt.Attempts {
			return fmt.Errorf("maintenance: claims partition %s still busy after %d attempts: %w", p.QualifiedName(), attempt, err)
		}
		log.Warn("claims partition busy, retrying", "partition", p.QualifiedName(), "attempt", attempt,
			"lock_timeout_ms", opt.LockTimeout.Milliseconds())
		t := time.NewTimer(opt.RetryDelay)
		select {
		case <-ctx.Done():
			t.Stop()
			return errors.Join(err, context.Cause(ctx))
		case <-t.C:
		}
	}
}

// VacuumTables are the tables the pipeline updates, vacuumed in this order,
// one statement each: raw.api_calls (each exported row is stamped once), the
// export ledger (two updates per window and per part) and the transformer's
// batch manifests.
var VacuumTables = []string{"raw.api_calls", "pipeline.export_windows", "pipeline.export_batches", "pipeline.consumer_batches"}

// Vacuum runs VACUUM (ANALYZE) on VacuumTables, one autocommit statement per
// table on one dedicated connection (VACUUM cannot run inside a transaction
// block, nor share an Exec with another statement). The connection is
// hijacked from the pool and closed at the end, so its session settings never
// reach the pool: transaction_timeout and statement_timeout are switched off
// on it, because the database default transaction_timeout (5min, migration
// 00002) would terminate a long VACUUM. On the partitioned parent
// raw.api_calls, VACUUM covers every partition, and ANALYZE also refreshes
// the parent-level statistics autovacuum never collects.
func Vacuum(ctx context.Context, pool *pgxpool.Pool, log *slog.Logger) error {
	c, err := pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("maintenance: vacuum: acquire connection: %w", err)
	}
	conn := c.Hijack()
	defer func() { _ = conn.Close(context.WithoutCancel(ctx)) }()
	var version int
	if err := conn.QueryRow(ctx, "SELECT current_setting('server_version_num')::int").Scan(&version); err != nil {
		return fmt.Errorf("maintenance: vacuum: server version: %w", err)
	}
	for _, set := range noTimeouts(version) {
		if _, err := conn.Exec(ctx, set); err != nil {
			return fmt.Errorf("maintenance: vacuum: %s: %w", set, err)
		}
	}
	for _, table := range VacuumTables {
		t0 := time.Now()
		// table names are constants, never input.
		if _, err := conn.Exec(ctx, "VACUUM (ANALYZE) "+table); err != nil {
			return fmt.Errorf("maintenance: vacuum %s: %w", table, err)
		}
		log.Info("vacuumed", "table", table, "duration_ms", time.Since(t0).Milliseconds())
	}
	return nil
}

// noTimeouts returns the statements that switch the session timeouts off for
// server_version_num version. transaction_timeout exists only from
// PostgreSQL 17 on; migration 00002 guards it the same way.
func noTimeouts(version int) []string {
	if version >= 170000 {
		return []string{"SET transaction_timeout = 0", "SET statement_timeout = 0"}
	}
	return []string{"SET statement_timeout = 0"}
}
