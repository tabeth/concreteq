package service

import (
	"testing"

	"github.com/tabeth/concretedb/models"
)

func TestCalculateReadCapacity(t *testing.T) {
	tests := []struct {
		size       int
		consistent bool
		expected   float64
	}{
		{0, true, 1.0},
		{4096, true, 1.0},
		{4097, true, 2.0},
		{1, false, 0.5},
		{4096, false, 0.5},
		{4097, false, 1.0},
	}

	for _, tt := range tests {
		got := CalculateReadCapacity(tt.size, tt.consistent)
		if got != tt.expected {
			t.Errorf("CalculateReadCapacity(%d, %v) = %f; want %f", tt.size, tt.consistent, got, tt.expected)
		}
	}
}

func TestCalculateWriteCapacity(t *testing.T) {
	tests := []struct {
		size     int
		expected float64
	}{
		{0, 1.0},
		{1024, 1.0},
		{1025, 2.0},
	}

	for _, tt := range tests {
		got := CalculateWriteCapacity(tt.size)
		if got != tt.expected {
			t.Errorf("CalculateWriteCapacity(%d) = %f; want %f", tt.size, got, tt.expected)
		}
	}
}

func TestEstimateAttributeValueSize(t *testing.T) {
	s := "test"
	n := "123"
	b := "dGhpcyBpcyBhIHRlc3Q="
	bt := true
	nl := true

	tests := []struct {
		name     string
		av       models.AttributeValue
		expected int
	}{
		{"String", models.AttributeValue{S: &s}, 4},
		{"Number", models.AttributeValue{N: &n}, 3},
		{"Binary", models.AttributeValue{B: &b}, 20},
		{"Bool", models.AttributeValue{BOOL: &bt}, 1},
		{"Null", models.AttributeValue{NULL: &nl}, 1},
		{"StringSet", models.AttributeValue{SS: []string{"a", "bb"}}, 3},
		{"NumberSet", models.AttributeValue{NS: []string{"1", "22"}}, 3},
		{"BinarySet", models.AttributeValue{BS: []string{"abc", "de"}}, 5},
		{"Map", models.AttributeValue{M: map[string]models.AttributeValue{"k": {S: &s}}}, 1 + 4},
		{"List", models.AttributeValue{L: []models.AttributeValue{{S: &s}, {N: &n}}}, 4 + 3},
		{"EmptyMap", models.AttributeValue{M: map[string]models.AttributeValue{}}, 0},
		{"EmptyList", models.AttributeValue{L: []models.AttributeValue{}}, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := EstimateAttributeValueSize(tt.av)
			if got != tt.expected {
				t.Errorf("EstimateAttributeValueSize() = %d; want %d", got, tt.expected)
			}
		})
	}
}

func TestEstimateItemSize(t *testing.T) {
	s := "val"
	item := map[string]models.AttributeValue{
		"pk": {S: &s},
	}
	// "pk" (2) + "val" (3) = 5
	expected := 5
	got := EstimateItemSize(item)
	if got != expected {
		t.Errorf("EstimateItemSize() = %d; want %d", got, expected)
	}

	if EstimateItemSize(nil) != 0 {
		t.Error("EstimateItemSize(nil) should be 0")
	}
}

func TestBuildConsumedCapacity(t *testing.T) {
	cc := BuildConsumedCapacity("table", 1.5, true)
	if cc.TableName != "table" || cc.CapacityUnits != 1.5 || cc.ReadCapacityUnits != 1.5 || cc.WriteCapacityUnits != 0 {
		t.Errorf("BuildConsumedCapacity(read) failed: %+v", cc)
	}

	ccw := BuildConsumedCapacity("table", 2.0, false)
	if ccw.WriteCapacityUnits != 2.0 || ccw.ReadCapacityUnits != 0 {
		t.Errorf("BuildConsumedCapacity(write) failed: %+v", ccw)
	}
}

func TestBuildItemCollectionMetrics(t *testing.T) {
	key := map[string]models.AttributeValue{"pk": {S: strPtr("1")}}
	icm := BuildItemCollectionMetrics("table", key)
	if icm.ItemCollectionKey["pk"].S == nil || *icm.ItemCollectionKey["pk"].S != "1" {
		t.Errorf("BuildItemCollectionMetrics failed: %+v", icm)
	}
}
