package transform

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
)

// ContentSHA256 hashes the canonical JSON of a Business struct. encoding/json
// writes struct fields in declaration order and Dec/float64 values in one
// canonical form, so equal business content always hashes equally. Business
// structs contain no technical columns (_event_id, _kafka_offset, fetched_at, …),
// so the same coin snapshot arriving in two different messages hashes the same.
func ContentSHA256(business any) ([32]byte, error) {
	b, err := json.Marshal(business)
	if err != nil {
		return [32]byte{}, fmt.Errorf("transform: hash: %w", err)
	}
	return sha256.Sum256(b), nil
}
