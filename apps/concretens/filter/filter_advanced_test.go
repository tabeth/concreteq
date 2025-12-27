package filter

import (
	"testing"
)

func TestMatches_Advanced(t *testing.T) {
	tests := []struct {
		name        string
		policy      string
		messageData interface{} // map[string]string or map[string]interface{}
		want        bool
	}{
		{
			name:   "Basic String Attribute Match",
			policy: `{"store": ["example_corp"]}`,
			messageData: map[string]string{
				"store": "example_corp",
			},
			want: true,
		},
		{
			name:   "Nested JSON Match",
			policy: `{"customer": {"source": ["web"]}}`,
			messageData: map[string]interface{}{
				"customer": map[string]interface{}{
					"source": "web",
				},
			},
			want: true,
		},
		{
			name:   "Nested JSON Mismatch",
			policy: `{"customer": {"source": ["web"]}}`,
			messageData: map[string]interface{}{
				"customer": map[string]interface{}{
					"source": "mobile",
				},
			},
			want: false,
		},
		{
			name:   "Deep Nesting",
			policy: `{"a": {"b": {"c": ["d"]}}}`,
			messageData: map[string]interface{}{
				"a": map[string]interface{}{
					"b": map[string]interface{}{
						"c": "d",
					},
				},
			},
			want: true,
		},
		{
			name:   "Numeric Match",
			policy: `{"price": [100]}`,
			messageData: map[string]interface{}{
				"price": 100.0, // JSON unmarshal uses float64
			},
			want: true,
		},
		{
			name:   "Numeric Mismatch",
			policy: `{"price": [100]}`,
			messageData: map[string]interface{}{
				"price": 99.0,
			},
			want: false,
		},
		{
			name:        "Empty Policy Matches Everything",
			policy:      `{}`,
			messageData: map[string]interface{}{"foo": "bar"},
			want:        true,
		},
		{
			name:   "Missing Key",
			policy: `{"required": ["val"]}`,
			messageData: map[string]interface{}{
				"other": "val",
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Matches(tt.policy, tt.messageData)
			if err != nil {
				t.Errorf("Matches() error = %v", err)
				return
			}
			if got != tt.want {
				t.Errorf("Matches() = %v, want %v", got, tt.want)
			}
		})
	}
}
