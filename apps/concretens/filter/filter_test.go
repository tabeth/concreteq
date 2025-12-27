package filter

import (
	"testing"
)

func TestMatches(t *testing.T) {
	tests := []struct {
		name      string
		policy    string
		attrs     map[string]string
		wantMatch bool
		wantError bool
	}{
		{
			name:      "Empty policy matches everything",
			policy:    "",
			attrs:     map[string]string{"color": "red"},
			wantMatch: true,
		},
		{
			name:      "Empty JSON policy matches everything",
			policy:    "{}",
			attrs:     map[string]string{"color": "red"},
			wantMatch: true,
		},
		{
			name:      "Single attribute exact match",
			policy:    `{"store": ["example_corp"]}`,
			attrs:     map[string]string{"store": "example_corp", "event": "order"},
			wantMatch: true,
		},
		{
			name:      "Single attribute mismatch",
			policy:    `{"store": ["example_corp"]}`,
			attrs:     map[string]string{"store": "other_corp", "event": "order"},
			wantMatch: false,
		},
		{
			name:      "Attribute missing in message",
			policy:    `{"store": ["example_corp"]}`,
			attrs:     map[string]string{"event": "order"},
			wantMatch: false,
		},
		{
			name:      "OR logic match (first)",
			policy:    `{"color": ["red", "blue"]}`,
			attrs:     map[string]string{"color": "red"},
			wantMatch: true,
		},
		{
			name:      "OR logic match (second)",
			policy:    `{"color": ["red", "blue"]}`,
			attrs:     map[string]string{"color": "blue"},
			wantMatch: true,
		},
		{
			name:      "OR logic mismatch",
			policy:    `{"color": ["red", "blue"]}`,
			attrs:     map[string]string{"color": "green"},
			wantMatch: false,
		},
		{
			name:      "Multiple attributes match",
			policy:    `{"color": ["red"], "size": ["large"]}`,
			attrs:     map[string]string{"color": "red", "size": "large"},
			wantMatch: true,
		},
		{
			name:      "Multiple attributes one mismatch",
			policy:    `{"color": ["red"], "size": ["large"]}`,
			attrs:     map[string]string{"color": "red", "size": "small"},
			wantMatch: false,
		},
		{
			name:      "Invalid Policy JSON",
			policy:    `{invalid`,
			attrs:     map[string]string{"color": "red"},
			wantMatch: false,
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Matches(tt.policy, tt.attrs)
			if (err != nil) != tt.wantError {
				t.Errorf("Matches() error = %v, wantError %v", err, tt.wantError)
				return
			}
			if got != tt.wantMatch {
				t.Errorf("Matches() = %v, want %v", got, tt.wantMatch)
			}
		})
	}
}
