package event

import (
	"encoding/json"
	"testing"
)

func TestCancelKey(t *testing.T) {
	tests := []struct {
		crawlID int64
		want    string
	}{
		{1, "aranea:cancel:1"},
		{0, "aranea:cancel:0"},
		{999999, "aranea:cancel:999999"},
	}
	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			got := CancelKey(tt.crawlID)
			if got != tt.want {
				t.Errorf("CancelKey(%d) = %q, want %q", tt.crawlID, got, tt.want)
			}
		})
	}
}

func TestDLQMessageMarshal(t *testing.T) {
	// DLQMessage должен маршалиться даже с пустым payload —
	// это защита от ядовитых событий, которые могут быть невалидным JSON.
	msg := DLQMessage{
		Topic:    "aranea.tasks",
		Error:    "invalid json",
		Attempts: 3,
		Time:     "2024-01-01T00:00:00Z",
		Payload:  "", // пустой payload — допустим
	}

	b, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("Marshal(DLQMessage) error: %v", err)
	}

	// Проверяем, что результат — валидный JSON
	var decoded map[string]any
	if err := json.Unmarshal(b, &decoded); err != nil {
		t.Fatalf("Round-trip unmarshal error: %v", err)
	}

	if decoded["topic"] != "aranea.tasks" {
		t.Errorf("topic = %v, want %q", decoded["topic"], "aranea.tasks")
	}
	if decoded["attempts"] != float64(3) {
		t.Errorf("attempts = %v, want 3", decoded["attempts"])
	}
}

func TestTaskMarshalRoundTrip(t *testing.T) {
	maxDepth := 5
	task := Task{
		CrawlID:  42,
		URL:      "https://example.com/page",
		Depth:    2,
		MaxDepth: &maxDepth,
	}

	b, err := json.Marshal(task)
	if err != nil {
		t.Fatalf("Marshal(Task) error: %v", err)
	}

	var decoded Task
	if err := json.Unmarshal(b, &decoded); err != nil {
		t.Fatalf("Unmarshal(Task) error: %v", err)
	}

	if decoded.CrawlID != task.CrawlID || decoded.URL != task.URL || decoded.Depth != task.Depth {
		t.Errorf("round-trip mismatch: got %+v, want %+v", decoded, task)
	}
	if decoded.MaxDepth == nil || *decoded.MaxDepth != *task.MaxDepth {
		t.Errorf("MaxDepth mismatch: got %v, want %v", decoded.MaxDepth, task.MaxDepth)
	}
}

func TestPageResultMarshalRoundTrip(t *testing.T) {
	maxDepth := 3
	result := PageResult{
		CrawlID:    1,
		URL:        "https://example.com",
		Depth:      0,
		StatusCode: 200,
		LatencyMs:  150,
		Title:      "Test",
		Text:       "Some text here",
		Links:      []string{"https://example.com/1", "https://example.com/2"},
		MaxDepth:   &maxDepth,
	}

	b, err := json.Marshal(result)
	if err != nil {
		t.Fatalf("Marshal(PageResult) error: %v", err)
	}

	var decoded PageResult
	if err := json.Unmarshal(b, &decoded); err != nil {
		t.Fatalf("Unmarshal(PageResult) error: %v", err)
	}

	if decoded.CrawlID != result.CrawlID || decoded.URL != result.URL {
		t.Errorf("round-trip mismatch: got %+v", decoded)
	}
	if len(decoded.Links) != len(result.Links) {
		t.Errorf("links length: got %d, want %d", len(decoded.Links), len(result.Links))
	}
}

func TestTaskMarshalOmitEmpty(t *testing.T) {
	// Task без MaxDepth — поле должно опускаться
	task := Task{
		CrawlID: 1,
		URL:     "https://example.com",
		Depth:   0,
		// MaxDepth nil — должно опуститься
	}

	b, err := json.Marshal(task)
	if err != nil {
		t.Fatalf("Marshal(Task) error: %v", err)
	}

	// Проверяем, что "max_depth" отсутствует в JSON
	var decoded map[string]any
	if err := json.Unmarshal(b, &decoded); err != nil {
		t.Fatalf("Unmarshal error: %v", err)
	}
	if _, ok := decoded["max_depth"]; ok {
		t.Error("max_depth should be omitted when nil")
	}
}
