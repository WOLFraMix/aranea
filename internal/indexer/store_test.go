package indexer

import (
	"fmt"
	"testing"
)

func TestSeenKey(t *testing.T) {
	tests := []struct {
		crawlID int64
		want    string
	}{
		{1, "aranea:seen:1"},
		{0, "aranea:seen:0"},
		{999999, "aranea:seen:999999"},
	}
	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			got := seenKey(tt.crawlID)
			if got != tt.want {
				t.Errorf("seenKey(%d) = %q, want %q", tt.crawlID, got, tt.want)
			}
		})
	}
}

// TestSeenKeyFormat проверяет, что ключи не конфликтуют между краулами.
func TestSeenKeyUniqueness(t *testing.T) {
	seen := make(map[string]bool)
	for id := int64(1); id <= 100; id++ {
		key := seenKey(id)
		if seen[key] {
			t.Errorf("duplicate key for crawl %d: %s", id, key)
		}
		seen[key] = true
	}
}

// TestSeenKeyPattern проверяет формат ключа.
func TestSeenKeyPattern(t *testing.T) {
	key := seenKey(42)
	expected := fmt.Sprintf("aranea:seen:%d", 42)
	if key != expected {
		t.Errorf("seenKey(42) = %q, want %q", key, expected)
	}
}
