package roomcore

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	aetherv1 "github.com/SamarthParnami/aether/go/gen/aether/v1"
)

// goldenFile is the shared, language-neutral fixture. The TS SDK reducer (PR-20)
// reads the same file, so Go and TS can never drift on what an event means.
var goldenFile = filepath.Join("..", "..", "..", "testdata", "golden", "roomcore.json")

type goldenSuite struct {
	Cases []struct {
		Name   string `json:"name"`
		Events []struct {
			Key   string `json:"key"`
			Value string `json:"value"`
		} `json:"events"`
		Expected map[string]string `json:"expected"`
	} `json:"cases"`
}

func TestGoldenVectors(t *testing.T) {
	data, err := os.ReadFile(goldenFile)
	if err != nil {
		t.Fatalf("read golden file: %v", err)
	}
	var suite goldenSuite
	if err := json.Unmarshal(data, &suite); err != nil {
		t.Fatalf("parse golden file: %v", err)
	}
	if len(suite.Cases) == 0 {
		t.Fatal("no golden cases found")
	}

	for _, tc := range suite.Cases {
		t.Run(tc.Name, func(t *testing.T) {
			events := make([]*aetherv1.Event, len(tc.Events))
			for i, e := range tc.Events {
				events[i] = kvEvent(uint64(i+1), e.Key, e.Value)
			}

			got := Replay(emptyState(), events).GetEntries()
			if len(got) != len(tc.Expected) {
				t.Fatalf("entry count = %d, want %d (got %v)", len(got), len(tc.Expected), got)
			}
			for k, want := range tc.Expected {
				if string(got[k]) != want {
					t.Errorf("entries[%q] = %q, want %q", k, string(got[k]), want)
				}
			}
		})
	}
}
