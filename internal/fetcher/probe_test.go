package fetcher

import (
	"context"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/WOLFraMix/aranea/internal/event"
)

// --- isPrivateIP ---

func TestIsPrivateIP(t *testing.T) {
	tests := []struct {
		input  string
		expect bool
	}{
		// Loopback
		{"127.0.0.1", true},
		{"127.0.0.255", true},
		{"::1", true},
		// Link-local
		{"169.254.0.1", true},
		{"169.254.255.255", true},
		// RFC 1918
		{"10.0.0.1", true},
		{"10.255.255.255", true},
		{"172.16.0.1", true},
		{"172.31.255.255", true},
		{"192.168.0.1", true},
		{"192.168.255.255", true},
		// Carrier-grade NAT
		{"100.64.0.1", true},
		{"100.127.255.255", true},
		// Public IPs — должны пропустить
		{"8.8.8.8", false},
		{"1.1.1.1", false},
		{"203.0.113.5", false},
		{"172.15.255.255", false}, // just outside 172.16/12
		{"172.32.0.0", false},     // just outside 172.16/12
		{"192.167.0.1", false},    // just outside 192.168/16
		// Non-IP
		{"not-an-ip", false},
		{"", false},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := isPrivateIP(tt.input)
			if got != tt.expect {
				t.Errorf("isPrivateIP(%q) = %v, want %v", tt.input, got, tt.expect)
			}
		})
	}
}

// --- extractTitle ---

func TestExtractTitle(t *testing.T) {
	tests := []struct {
		name string
		html string
		want string
	}{
		{
			name: "normal title",
			html: "<html><head><title>Hello World</title></head><body>ok</body></html>",
			want: "Hello World",
		},
		{
			name: "title with whitespace",
			html: "<html><head><title>  Spaced  </title></head><body></body></html>",
			want: "Spaced",
		},
		{
			name: "no title tag",
			html: "<html><body>no title</body></html>",
			want: "",
		},
		{
			name: "empty title",
			html: "<html><head><title></title></head><body></body></html>",
			want: "",
		},
		{
			name: "title with nested tags — text from all tags included",
			html: "<html><head><title>Before <em>Inner</em> After</title></head><body></body></html>",
			want: "Before <em>Inner</em> After",
		},
		{
			name: "title with meta tags before",
			html: "<html><head><meta charset=\"utf-8\"><title>Page</title></head><body></body></html>",
			want: "Page",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := extractTitle(strings.NewReader(tt.html))
			if got != tt.want {
				t.Errorf("extractTitle() = %q, want %q", got, tt.want)
			}
		})
	}
}

// --- extractBodyText ---

func TestExtractBodyText(t *testing.T) {
	tests := []struct {
		name string
		html string
		want string
	}{
		{
			name: "single paragraph",
			html: "<html><body><p>One paragraph.</p></body></html>",
			want: "One paragraph.",
		},
		{
			name: "multiple paragraphs",
			html: "<html><body><p>First.</p><p>Second.</p></body></html>",
			want: "First. Second.",
		},
		{
			name: "empty paragraphs ignored",
			html: "<html><body><p>   </p><p>Real text</p></body></html>",
			want: "Real text",
		},
		{
			name: "text outside p ignored",
			html: "<html><body>outside<p>inside</p></body></html>",
			want: "inside",
		},
		{
			name: "nested tags inside p — text concatenated with spaces",
			html: "<html><body><p>Hello <strong>world</strong>!</p></body></html>",
			want: "Hello world !",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := extractBodyText(strings.NewReader(tt.html))
			if got != tt.want {
				t.Errorf("extractBodyText() = %q, want %q", got, tt.want)
			}
		})
	}
}

// --- extractDescription ---

func TestExtractDescription(t *testing.T) {
	tests := []struct {
		name string
		html string
		want string
	}{
		{
			name: "standard meta description",
			html: `<html><head><meta name="description" content="A good description"></head><body></body></html>`,
			want: "A good description",
		},
		{
			name: "description with whitespace",
			html: `<html><head><meta name="description" content="  Spaced  "></head><body></body></html>`,
			want: "Spaced",
		},
		{
			name: "no description",
			html: `<html><head></head><body></body></html>`,
			want: "",
		},
		{
			name: "other meta tag ignored",
			html: `<html><head><meta name="keywords" content="foo"></head><body></body></html>`,
			want: "",
		},
		{
			name: "case-insensitive name",
			html: `<html><head><meta NAME="DESCRIPTION" content="upper"></head><body></body></html>`,
			want: "upper",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := extractDescription(strings.NewReader(tt.html))
			if got != tt.want {
				t.Errorf("extractDescription() = %q, want %q", got, tt.want)
			}
		})
	}
}

// --- extractLinks (extended) ---

func TestExtractLinksFilters(t *testing.T) {
	base, err := url.Parse("https://example.com/page")
	if err != nil {
		t.Fatal(err)
	}

	html := `<html><body>
		<a href="https://other.com/ok">OK</a>
		<a href="https://other.com/ok">Duplicate</a>
		<a href="mailto:x@y.com">Mail</a>
		<a href="javascript:alert(1)">JS</a>
		<a href="ftp://ftp.example.com/file">FTP</a>
		<a href="#fragment">Frag</a>
		<a href="/relative">Rel</a>
		<a href="../parent">Parent</a>
		<a href="">Empty</a>
		<a>no href</a>
		<a href="https://other.com/ok?query=1">Query</a>
	</body></html>`

	got := extractLinks(base, strings.NewReader(html))

	// #fragment resolves to base URL (https://example.com/page) after stripping fragment
	// ../parent resolves to https://example.com/parent
	expected := map[string]bool{
		"https://other.com/ok":         true,
		"https://other.com/ok?query=1": true,
		"https://example.com/relative": true,
		"https://example.com/parent":   true,
		"https://example.com/page":     true, // #fragment resolves to base
	}

	if len(got) != len(expected) {
		t.Errorf("got %d links, expected %d: %v", len(got), len(expected), got)
	}
	for _, link := range got {
		if !expected[link] {
			t.Errorf("unexpected link: %s", link)
		}
	}
}

// --- parseRobots ---

func TestParseRobots(t *testing.T) {
	tests := []struct {
		name    string
		robots  string
		ua      string
		path    string
		allowed bool
	}{
		{
			name:    "disallow all",
			robots:  "User-agent: *\nDisallow: /\n",
			ua:      "aranea",
			path:    "/",
			allowed: false,
		},
		{
			name:    "allow specific path",
			robots:  "User-agent: *\nDisallow: /\nAllow: /public/\n",
			ua:      "aranea",
			path:    "/public/page",
			allowed: true,
		},
		{
			name:    "longer prefix wins",
			robots:  "User-agent: *\nDisallow: /\nAllow: /public/admin/\n",
			ua:      "aranea",
			path:    "/public/admin/settings",
			allowed: true,
		},
		{
			name:    "ua-specific rule",
			robots:  "User-agent: googlebot\nDisallow: /\n\nUser-agent: aranea\nAllow: /\n",
			ua:      "aranea",
			path:    "/secret",
			allowed: true,
		},
		{
			name:    "empty disallow means allow all",
			robots:  "User-agent: *\nDisallow:\n",
			ua:      "aranea",
			path:    "/anything",
			allowed: true,
		},
		{
			name:    "comments and blank lines",
			robots:  "# This is a comment\n\nUser-agent: *\n# Another comment\nDisallow: /admin\n",
			ua:      "aranea",
			path:    "/admin",
			allowed: false,
		},
		{
			name:    "ua partial match — aranea matches aranea/1.0",
			robots:  "User-agent: aranea/1.0\nDisallow: /\n",
			ua:      "aranea",
			path:    "/",
			allowed: false, // aranea matches aranea/1.0, Disallow: / wins
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rs := parseRobots(strings.NewReader(tt.robots), tt.ua)
			got := checkRobotsPath(rs, tt.path)
			if got != tt.allowed {
				t.Errorf("Allow(%q) = %v, want %v", tt.path, got, tt.allowed)
			}
		})
	}
}

// checkRobotsPath — helper that mimics the longest-prefix matching from Robots.Allow
func checkRobotsPath(rs *ruleSet, path string) bool {
	if path == "" {
		path = "/"
	}
	best := -1
	allowed := true
	for i, ru := range rs.rules {
		if !strings.HasPrefix(path, ru.prefix) {
			continue
		}
		if best == -1 ||
			len(ru.prefix) > len(rs.rules[best].prefix) ||
			(len(ru.prefix) == len(rs.rules[best].prefix) && ru.allow) {
			best = i
			allowed = ru.allow
		}
	}
	return allowed
}

// --- DomainLimits ---

func TestDomainLimitsWait(t *testing.T) {
	ctx := context.Background()
	dl := NewDomainLimits(ctx, 100) // 100 req/s per domain
	defer dl.Close()

	// Два разных домена — не должны мешать друг другу
	errCh := make(chan error, 2)
	go func() { errCh <- dl.Wait(ctx, "https://alpha.com/page") }()
	go func() { errCh <- dl.Wait(ctx, "https://beta.com/page") }()

	for i := 0; i < 2; i++ {
		if err := <-errCh; err != nil {
			t.Errorf("Wait() error: %v", err)
		}
	}
}

func TestDomainLimitsCaseInsensitive(t *testing.T) {
	ctx := context.Background()
	dl := NewDomainLimits(ctx, 100)
	defer dl.Close()

	// Разный регистр хоста — один и тот же домен, один лимитер
	if err := dl.Wait(ctx, "https://Example.COM/page"); err != nil {
		t.Errorf("Wait(Example.COM) error: %v", err)
	}
	if err := dl.Wait(ctx, "https://example.com/page"); err != nil {
		t.Errorf("Wait(example.com) error: %v", err)
	}
}

func TestDomainLimitsContextCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	dl := NewDomainLimits(ctx, 1) // 1 req/s — медленно

	cancel() // отменяем сразу
	err := dl.Wait(ctx, "https://slow.com/page")
	if err == nil {
		t.Error("Wait() should error when context is cancelled")
	}
	dl.Close()
}

func TestDomainLimitsInvalidURL(t *testing.T) {
	ctx := context.Background()
	dl := NewDomainLimits(ctx, 100)
	defer dl.Close()

	err := dl.Wait(ctx, "not-a-url")
	if err == nil {
		t.Error("Wait() should error for invalid URL")
	}
}

// --- Robots.Close ---

func TestRobotsClose(t *testing.T) {
	ctx := context.Background()
	rb := NewRobots(ctx, 5*time.Second)
	rb.Close() // не должен паниковать при двойном вызове
	rb.Close()
}

// --- Probe ---

func TestNewProbeHasTimeout(t *testing.T) {
	probe := NewProbe(5 * time.Second)
	if probe.client.Timeout != 5*time.Second {
		t.Errorf("Timeout = %v, want 5s", probe.client.Timeout)
	}
	if probe.client.Transport == nil {
		t.Error("Transport is nil")
	}
}

func TestProbeFetchBadURL(t *testing.T) {
	probe := NewProbe(5 * time.Second)
	ctx := context.Background()

	res := probe.Fetch(ctx, event.Task{URL: "not-a-url", Depth: 0})
	if res.Error == "" {
		t.Error("Fetch should return error for bad URL")
	}
}

func TestProbeFetchMalformedURL(t *testing.T) {
	probe := NewProbe(5 * time.Second)
	ctx := context.Background()

	res := probe.Fetch(ctx, event.Task{URL: "ftp://example.com", Depth: 0})
	// URL валидный, но scheme не http/https — probe попытается сделать GET
	// и получит ошибку (нет сервера), но не паникует
	if res.Error == "" {
		// Если вдруг сервер есть — ок, главное не паника
		t.Log("Fetch returned no error for ftp:// — unexpected but not a panic")
	}
}
