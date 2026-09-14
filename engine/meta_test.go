package main

import "testing"

func TestParseMetaCookies(t *testing.T) {
	want := map[string]string{"sessionid": "abc", "csrftoken": "xyz", "mid": "m1"}
	eq := func(t *testing.T, got map[string]string) {
		for k, v := range want {
			if got[k] != v {
				t.Fatalf("cookie %q = %q, want %q (got %v)", k, got[k], v, got)
			}
		}
	}
	cases := map[string]string{
		"chrome -b":       `curl 'https://www.instagram.com/' -b 'sessionid=abc; csrftoken=xyz; mid=m1' -H 'user-agent: x'`,
		"chrome --cookie": `curl 'https://www.instagram.com/' --cookie "sessionid=abc; csrftoken=xyz; mid=m1"`,
		"firefox -H":      `curl 'https://www.instagram.com/' -H 'Cookie: sessionid=abc; csrftoken=xyz; mid=m1'`,
		"json":            `{"sessionid":"abc","csrftoken":"xyz","mid":"m1"}`,
		"raw header":      `Cookie: sessionid=abc; csrftoken=xyz; mid=m1`,
		"bare string":     `sessionid=abc; csrftoken=xyz; mid=m1`,
	}
	for name, raw := range cases {
		t.Run(name, func(t *testing.T) {
			got, err := parseMetaCookies(raw)
			if err != nil {
				t.Fatalf("parse error: %v", err)
			}
			eq(t, got)
		})
	}

	if _, err := parseMetaCookies(""); err == nil {
		t.Fatal("empty input should error")
	}
	if _, err := parseMetaCookies("not a cookie or curl"); err == nil {
		t.Fatal("garbage input should error")
	}
}
