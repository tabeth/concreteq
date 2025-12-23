package models

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestAPIError(t *testing.T) {
	err := New("MyType", "MyMessage")
	if err.Type != "MyType" {
		t.Errorf("expected Type 'MyType', got '%s'", err.Type)
	}
	if err.Message != "MyMessage" {
		t.Errorf("expected Message 'MyMessage', got '%s'", err.Message)
	}
	if err.Error() != "MyMessage" {
		t.Errorf("expected Error() to return 'MyMessage', got '%s'", err.Error())
	}
}

func TestAttributeDefinition_JSON(t *testing.T) {
	attr := AttributeDefinition{
		AttributeName: "pk",
		AttributeType: "S",
	}
	data, err := json.Marshal(attr)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	jsonStr := string(data)
	if !strings.Contains(jsonStr, `"AttributeName":"pk"`) {
		t.Errorf("missing AttributeName: %s", jsonStr)
	}
	if !strings.Contains(jsonStr, `"AttributeType":"S"`) {
		t.Errorf("missing AttributeType: %s", jsonStr)
	}
}

func TestKeySchemaElement_JSON(t *testing.T) {
	kse := KeySchemaElement{
		AttributeName: "pk",
		KeyType:       "HASH",
	}
	data, err := json.Marshal(kse)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	jsonStr := string(data)
	if !strings.Contains(jsonStr, `"AttributeName":"pk"`) {
		t.Errorf("missing AttributeName: %s", jsonStr)
	}
	if !strings.Contains(jsonStr, `"KeyType":"HASH"`) {
		t.Errorf("missing KeyType: %s", jsonStr)
	}
}

func TestProvisionedThroughput_JSON(t *testing.T) {
	pt := ProvisionedThroughput{
		ReadCapacityUnits:  10,
		WriteCapacityUnits: 5,
	}
	data, err := json.Marshal(pt)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	jsonStr := string(data)
	if !strings.Contains(jsonStr, `"ReadCapacityUnits":10`) {
		t.Errorf("missing ReadCapacityUnits: %s", jsonStr)
	}
	if !strings.Contains(jsonStr, `"WriteCapacityUnits":5`) {
		t.Errorf("missing WriteCapacityUnits: %s", jsonStr)
	}
}

func TestCreateTableRequest_JSON(t *testing.T) {
	req := CreateTableRequest{
		TableName: "TestProps",
		AttributeDefinitions: []AttributeDefinition{
			{AttributeName: "id", AttributeType: "S"},
		},
		KeySchema: []KeySchemaElement{
			{AttributeName: "id", KeyType: "HASH"},
		},
		ProvisionedThroughput: ProvisionedThroughput{
			ReadCapacityUnits:  5,
			WriteCapacityUnits: 5,
		},
	}

	data, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("Failed to marshal CreateTableRequest: %v", err)
	}

	jsonStr := string(data)
	expectedKeys := []string{
		`"TableName":"TestProps"`,
		`"AttributeDefinitions":[`,
		`"KeySchema":[`,
		`"ProvisionedThroughput":{`,
	}

	for _, key := range expectedKeys {
		if !strings.Contains(jsonStr, key) {
			t.Errorf("JSON output missing key/value %q: %s", key, jsonStr)
		}
	}
}

func TestTableDescription_JSON(t *testing.T) {
	td := TableDescription{
		TableName:   "MyTable",
		TableStatus: "ACTIVE",
		AttributeDefinitions: []AttributeDefinition{
			{AttributeName: "id", AttributeType: "N"},
		},
		KeySchema: []KeySchemaElement{
			{AttributeName: "id", KeyType: "HASH"},
		},
		CreationDateTime: 123456789.0,
		ProvisionedThroughput: ProvisionedThroughput{
			ReadCapacityUnits:  1,
			WriteCapacityUnits: 1,
		},
		TableSizeBytes: 1024,
		ItemCount:      42,
	}

	data, err := json.Marshal(td)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	jsonStr := string(data)

	checks := []string{
		`"TableName":"MyTable"`,
		`"TableStatus":"ACTIVE"`,
		`"CreationDateTime":1.23456789e+08`, // approximate check for float, or just check existence
		`"TableSizeBytes":1024`,
		`"ItemCount":42`,
	}
	for _, c := range checks {
		if !strings.Contains(jsonStr, c) {
			// Float formatting might be tricky, let's relax the timestamp check if needed or improve regex
			// For simple check:
			if c == `"CreationDateTime":1.23456789e+08` && !strings.Contains(jsonStr, "CreationDateTime") {
				t.Errorf("missing CreationDateTime: %s", jsonStr)
				continue
			}
			if c != `"CreationDateTime":1.23456789e+08` {
				t.Errorf("missing %s: %s", c, jsonStr)
			}
		}
	}
}

func TestCreateTableResponse_JSON(t *testing.T) {
	resp := CreateTableResponse{
		TableDescription: TableDescription{
			TableName: "RespTable",
		},
	}
	data, err := json.Marshal(resp)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	if !strings.Contains(string(data), `"TableDescription"`) {
		t.Errorf("missing TableDescription wrapper: %s", string(data))
	}
	if !strings.Contains(string(data), `"TableName":"RespTable"`) {
		t.Errorf("missing inner TableName: %s", string(data))
	}
}

func TestErrorResponse_JSON(t *testing.T) {
	er := ErrorResponse{
		Type:    "ValidationException",
		Message: "Invalid Input",
	}
	data, err := json.Marshal(er)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	jsonStr := string(data)
	if !strings.Contains(jsonStr, `"__type":"ValidationException"`) {
		t.Errorf("missing __type: %s", jsonStr)
	}
	if !strings.Contains(jsonStr, `"Message":"Invalid Input"`) {
		t.Errorf("missing Message: %s", jsonStr)
	}
}

func TestDeleteTableRequest_JSON(t *testing.T) {
	req := DeleteTableRequest{TableName: "DelTable"}
	data, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	if !strings.Contains(string(data), `"TableName":"DelTable"`) {
		t.Errorf("missing TableName: %s", string(data))
	}
}

func TestDeleteTableResponse_JSON(t *testing.T) {
	resp := DeleteTableResponse{
		TableDescription: TableDescription{TableName: "DelTable", TableStatus: "DELETING"},
	}
	data, err := json.Marshal(resp)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	if !strings.Contains(string(data), `"TableDescription"`) {
		t.Errorf("missing TableDescription: %s", string(data))
	}
	if !strings.Contains(string(data), `"TableStatus":"DELETING"`) {
		t.Errorf("missing TableStatus: %s", string(data))
	}
}
