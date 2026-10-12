package transform

import (
	"strconv"

	"coingecko-etl/internal/event"
)

// responseID identifies the response a record came from: its idempotency_key
// (the same for byte-identical copies, so replays and CDN repeats still dedup
// as replay/duplicate_response), else its event_id.
func responseID(ev event.RawEvent) string {
	if ev.IdempotencyKey != nil && *ev.IdempotencyKey != "" {
		return *ev.IdempotencyKey
	}
	return ev.EventID
}

// fallbackKey is the natural key of a record with no id of its own (rejected
// as missing_id): "#<response id>|<index in payload>|<snapshot_at>". CoinGecko
// stamps every coin of one pass with the same last_updated, so the index alone
// is unique only within one message.
func fallbackKey(respID string, i int, snapAt string) string {
	return "#" + respID + "|" + strconv.Itoa(i) + "|" + snapAt
}
