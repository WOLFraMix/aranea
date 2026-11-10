package api

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
)

// --- clampInt ---

func TestClampInt(t *testing.T) {
	tests := []struct {
		name string
		s    string
		min  int
		max  int
		def  int
		want int
	}{
		{"valid in range", "50", 1, 100, 20, 50},
		{"valid at min", "1", 1, 100, 20, 1},
		{"valid at max", "100", 1, 100, 20, 100},
		{"below min", "-5", 1, 100, 20, 1},
		{"above max", "999", 1, 100, 20, 100},
		{"invalid empty", "", 1, 100, 20, 20},
		{"invalid non-numeric", "abc", 1, 100, 20, 20},
		{"zero", "0", 1, 100, 20, 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := clampInt(tt.s, tt.min, tt.max, tt.def)
			if got != tt.want {
				t.Errorf("clampInt(%q, %d, %d, %d) = %d, want %d",
					tt.s, tt.min, tt.max, tt.def, got, tt.want)
			}
		})
	}
}

// --- httpError ---

func TestHttpError(t *testing.T) {
	w := httptest.NewRecorder()
	httpError(w, http.StatusBadRequest, "test error")

	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want %d", w.Code, http.StatusBadRequest)
	}

	contentType := w.Header().Get("Content-Type")
	if contentType != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", contentType)
	}

	body := strings.TrimSpace(w.Body.String())
	if !strings.Contains(body, `"error"`) || !strings.Contains(body, `"test error"`) {
		t.Errorf("body = %q, expected error field with message", body)
	}
}

// --- writeJSON ---

func TestWriteJSON(t *testing.T) {
	w := httptest.NewRecorder()
	writeJSON(w, http.StatusCreated, map[string]any{"key": "value"})

	if w.Code != http.StatusCreated {
		t.Errorf("status = %d, want %d", w.Code, http.StatusCreated)
	}

	contentType := w.Header().Get("Content-Type")
	if contentType != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", contentType)
	}

	body := strings.TrimSpace(w.Body.String())
	if !strings.Contains(body, `"key"`) || !strings.Contains(body, `"value"`) {
		t.Errorf("body = %q, expected key/value", body)
	}
}

// --- URL validation logic from startCrawl ---

func TestURLValidation(t *testing.T) {
	tests := []struct {
		name    string
		rawURL  string
		wantErr bool
	}{
		{"valid https", "https://example.com", false},
		{"valid http", "http://example.com", false},
		{"valid with port", "https://example.com:8443/path", false},
		{"valid with query", "https://example.com?q=1", false},
		{"empty", "", true},
		{"no host", "https://", true},
		{"relative", "/page", true},
		{"ftp scheme", "ftp://example.com", true},
		{"invalid url", "://not-a-url", true},
		{"mailto", "mailto:test@example.com", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			u, err := url.Parse(tt.rawURL)
			gotErr := err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https")
			if gotErr != tt.wantErr {
				t.Errorf("URL validation for %q: gotErr=%v, wantErr=%v", tt.rawURL, gotErr, tt.wantErr)
			}
		})
	}
}

// --- offset parsing from crawlPages ---

func TestOffsetParsing(t *testing.T) {
	tests := []struct {
		name string
		s    string
		want int
	}{
		{"valid", "10", 10},
		{"zero", "0", 0},
		{"empty", "", 0},
		{"invalid", "abc", 0},
		{"negative", "-5", -5}, // отрицательный — SQLite обработает
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n, err := strconv.Atoi(tt.s)
			if err != nil {
				n = 0
			}
			if n != tt.want {
				t.Errorf("offset parsing %q = %d, want %d", tt.s, n, tt.want)
			}
		})
	}
}
