package fetcher

import (
	"net/url"
	"strings"
	"testing"
)

func TestExtractLinksExampleCom(t *testing.T) {
	page := `<!doctype html>
<html>
<head><title>Example Domain</title></head>
<body>
<div>
<h1>Example Domain</h1>
<p>This domain is for use in illustrative examples in documents.</p>
<p><a href="https://www.iana.org/domains/example">More information...</a></p>
</div>
</body>
</html>`
	base, err := url.Parse("https://example.com")
	if err != nil {
		t.Fatal(err)
	}
	got := extractLinks(base, strings.NewReader(page))
	if len(got) != 1 || got[0] != "https://www.iana.org/domains/example" {
		t.Fatalf("extractLinks вернула %v, ожидалась 1 ссылка на iana.org", got)
	}
}
