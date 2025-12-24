package store

import (
	"reflect"
	"testing"

	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"github.com/tabeth/concretedb/models"
)

func TestFoundationDBStore_toTupleElement(t *testing.T) {

	strVal := "test-string"
	numVal := "123.456"
	binVal := "base64encoded"

	tests := []struct {
		name    string
		av      models.AttributeValue
		want    tuple.TupleElement
		wantErr bool
	}{
		{
			name: "String",
			av:   models.AttributeValue{S: &strVal},
			want: "test-string",
		},
		{
			name: "Number",
			av:   models.AttributeValue{N: &numVal},
			want: "123.456",
		},
		{
			name: "Binary",
			av:   models.AttributeValue{B: &binVal},
			want: []byte("base64encoded"),
		},
		{
			name:    "Unsupported_Bool",
			av:      models.AttributeValue{BOOL: new(bool)}, // Just set non-nil
			wantErr: true,
		},
		{
			name:    "Unsupported_Null",
			av:      models.AttributeValue{NULL: new(bool)},
			wantErr: true,
		},
		{
			name:    "Empty",
			av:      models.AttributeValue{},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := toTupleElement(tt.av)
			if (err != nil) != tt.wantErr {
				t.Errorf("toTupleElement() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && !reflect.DeepEqual(got, tt.want) {
				t.Errorf("toTupleElement() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestFoundationDBStore_buildKeyTuple(t *testing.T) {
	s := &FoundationDBStore{}

	// Table definition
	table := &models.Table{
		TableName: "test-table",
		KeySchema: []models.KeySchemaElement{
			{AttributeName: "pk", KeyType: "HASH"},
			{AttributeName: "sk", KeyType: "RANGE"},
		},
	}

	strVal := "pk-val"
	numVal := "10"

	tests := []struct {
		name    string
		key     map[string]models.AttributeValue
		want    []tuple.TupleElement // Changed type
		wantErr bool
	}{
		{
			name: "Valid_CompositeKey",
			key: map[string]models.AttributeValue{
				"pk": {S: &strVal},
				"sk": {N: &numVal},
			},
			want: []tuple.TupleElement{"pk-val", "10"},
		},
		{
			name: "Missing_PartitionKey",
			key: map[string]models.AttributeValue{
				"sk": {N: &numVal},
			},
			wantErr: true,
		},
		{
			name: "Missing_SortKey",
			key: map[string]models.AttributeValue{
				"pk": {S: &strVal},
			},
			wantErr: true,
		},
		{
			name: "Invalid_Type_In_Key",
			key: map[string]models.AttributeValue{
				"pk": {BOOL: new(bool)}, // Invalid type for key
				"sk": {N: &numVal},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := s.buildKeyTuple(table, tt.key)
			if (err != nil) != tt.wantErr {
				t.Errorf("buildKeyTuple() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && !reflect.DeepEqual(got, tt.want) {
				t.Errorf("buildKeyTuple() got = %v, want %v", got, tt.want)
			}
		})
	}
}
