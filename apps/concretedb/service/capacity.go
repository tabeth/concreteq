package service

import (
	"math"

	"github.com/tabeth/concretedb/models"
)

// CalculateReadCapacity calculates RCU based on item size and consistency.
func CalculateReadCapacity(itemSize int, consistent bool) float64 {
	if itemSize == 0 {
		itemSize = 1 // Minimum 1 byte for calculation? No, DynamoDB says 4KB rounds up.
	}
	// Round up to nearest 4KB
	units := math.Ceil(float64(itemSize) / 4096.0)
	if !consistent {
		units = units * 0.5
	}
	return units
}

// CalculateWriteCapacity calculates WCU based on item size.
func CalculateWriteCapacity(itemSize int) float64 {
	if itemSize == 0 {
		return 1.0 // Minimum 1 WCU
	}
	// Round up to nearest 1KB
	return math.Ceil(float64(itemSize) / 1024.0)
}

// EstimateItemSize estimates the size of a DynamoDB item in bytes.
// This is a simplified version of the official DynamoDB algorithm.
func EstimateItemSize(item map[string]models.AttributeValue) int {
	if item == nil {
		return 0
	}
	size := 0
	for k, v := range item {
		size += len(k)
		size += EstimateAttributeValueSize(v)
	}
	return size
}

// EstimateAttributeValueSize estimates the size of an AttributeValue.
func EstimateAttributeValueSize(av models.AttributeValue) int {
	size := 0
	if av.S != nil {
		size += len(*av.S)
	} else if av.N != nil {
		size += len(*av.N)
	} else if av.B != nil {
		size += len(*av.B)
	} else if av.BOOL != nil {
		size += 1
	} else if av.NULL != nil {
		size += 1
	} else if len(av.SS) > 0 {
		for _, s := range av.SS {
			size += len(s)
		}
	} else if len(av.NS) > 0 {
		for _, n := range av.NS {
			size += len(n)
		}
	} else if len(av.BS) > 0 {
		for _, b := range av.BS {
			size += len(b)
		}
	} else if len(av.M) > 0 {
		for mk, mv := range av.M {
			size += len(mk)
			size += EstimateAttributeValueSize(mv)
		}
	} else if len(av.L) > 0 {
		for _, lv := range av.L {
			size += EstimateAttributeValueSize(lv)
		}
	}
	return size
}

// BuildConsumedCapacity constructs a models.ConsumedCapacity struct.
func BuildConsumedCapacity(tableName string, units float64, isRead bool) *models.ConsumedCapacity {
	cc := &models.ConsumedCapacity{
		TableName:     tableName,
		CapacityUnits: units,
	}
	if isRead {
		cc.ReadCapacityUnits = units
	} else {
		cc.WriteCapacityUnits = units
	}
	// For TOTAL, we only need Table field.
	cc.Table = &models.Capacity{
		CapacityUnits: units,
	}
	if isRead {
		cc.Table.ReadCapacityUnits = units
	} else {
		cc.Table.WriteCapacityUnits = units
	}
	return cc
}

// BuildItemCollectionMetrics constructs a models.ItemCollectionMetrics struct.
func BuildItemCollectionMetrics(tableName string, key map[string]models.AttributeValue) *models.ItemCollectionMetrics {
	// For MVP, we use the whole key as ItemCollectionKey.
	// In reality, it should only be the Partition Key.
	return &models.ItemCollectionMetrics{
		ItemCollectionKey:   key,
		SizeEstimateRangeGB: []float64{0.0, 0.01},
	}
}
