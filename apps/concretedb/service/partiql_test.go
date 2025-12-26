package service

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/tabeth/concretedb/models"
)

// Tests using shared MockStore from service_coverage_test.go

// Tests

func TestParsePartiQL_Select(t *testing.T) {
	params := []models.AttributeValue{}
	op, err := ParsePartiQL("SELECT * FROM \"TestTable\" WHERE pk='123'", params)
	assert.NoError(t, err)
	assert.Equal(t, "SELECT", op.Type)
	assert.Equal(t, "TestTable", op.TableName)
	val := "123"
	assert.Equal(t, models.AttributeValue{S: &val}, op.Key["pk"])
}

func TestParsePartiQL_Insert(t *testing.T) {
	params := []models.AttributeValue{
		{M: map[string]models.AttributeValue{
			"pk": {S: new(string)},
		}},
	}
	op, err := ParsePartiQL("INSERT INTO TestTable VALUE ?", params)
	assert.NoError(t, err)
	assert.Equal(t, "INSERT", op.Type)
	assert.NotNil(t, op.Item)
}

func TestParsePartiQL_Update(t *testing.T) {
	params := []models.AttributeValue{}
	// UPDATE table SET attr=? WHERE pk=?
	// But our simplified parser expects params for Set AND Where?
	// parseUpdate logic: match 2 is setClause, match 3 is whereClause.
	// UpdateExpr = "SET "+setClause.
	// Key = parseKeyCondition(whereClause).

	// Example: UPDATE TestTable SET a=1 WHERE pk='123'
	op, err := ParsePartiQL("UPDATE TestTable SET a=1 WHERE pk='123'", params)
	assert.NoError(t, err)
	assert.Equal(t, "UPDATE", op.Type)
	assert.Equal(t, "TestTable", op.TableName)
	assert.Equal(t, "SET a=1", op.UpdateExpr)
	val := "123"
	assert.Equal(t, models.AttributeValue{S: &val}, op.Key["pk"])
}

func TestParsePartiQL_Delete(t *testing.T) {
	params := []models.AttributeValue{}
	op, err := ParsePartiQL("DELETE FROM TestTable WHERE pk='123'", params)
	assert.NoError(t, err)
	assert.Equal(t, "DELETE", op.Type)
	assert.Equal(t, "TestTable", op.TableName)
	val := "123"
	assert.Equal(t, models.AttributeValue{S: &val}, op.Key["pk"])
}

func TestExecuteStatement_Select(t *testing.T) {
	mockStore := new(MockStore)
	svc := NewTableService(mockStore)

	ctx := context.Background()

	// Mock behavior for GetItem (since we map SELECT with PK to GetItem)
	expectedItem := map[string]models.AttributeValue{
		"pk": {S: new(string)},
	}
	mockStore.On("GetItem", ctx, "TestTable", mock.Anything, "", mock.Anything, mock.Anything).Return(expectedItem, nil)

	req := &models.ExecuteStatementRequest{
		Statement: "SELECT * FROM TestTable WHERE pk='123'",
	}
	resp, err := svc.ExecuteStatement(ctx, req)

	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.Equal(t, 1, len(resp.Items))
}

func TestExecuteStatement_Select_Scan(t *testing.T) {
	mockStore := new(MockStore)
	svc := NewTableService(mockStore)
	ctx := context.Background()

	// SELECT without WHERE -> Scan
	mockStore.On("Scan", ctx, "TestTable", "", "", mock.Anything, mock.Anything, int32(0), mock.Anything, false).
		Return([]map[string]models.AttributeValue{}, map[string]models.AttributeValue{}, nil)

	req := &models.ExecuteStatementRequest{
		Statement: "SELECT * FROM TestTable",
	}
	resp, err := svc.ExecuteStatement(ctx, req)

	assert.NoError(t, err)
	assert.NotNil(t, resp)
	mockStore.AssertCalled(t, "Scan", ctx, "TestTable", "", "", mock.Anything, mock.Anything, int32(0), mock.Anything, false)
}

func TestExecuteStatement_Insert(t *testing.T) {
	mockStore := new(MockStore)
	svc := NewTableService(mockStore)
	ctx := context.Background()

	mockStore.On("PutItem", ctx, "TestTable", mock.Anything, "", mock.Anything, mock.Anything, "").Return(map[string]models.AttributeValue{}, nil)

	pks := "123"
	params := []models.AttributeValue{
		{M: map[string]models.AttributeValue{
			"pk": {S: &pks},
		}},
	}
	req := &models.ExecuteStatementRequest{
		Statement:  "INSERT INTO TestTable VALUE ?",
		Parameters: params,
	}
	resp, err := svc.ExecuteStatement(ctx, req)
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	mockStore.AssertCalled(t, "PutItem", ctx, "TestTable", mock.Anything, "", mock.Anything, mock.Anything, "")
}

func TestExecuteStatement_Update(t *testing.T) {
	mockStore := new(MockStore)
	svc := NewTableService(mockStore)
	ctx := context.Background()

	mockStore.On("UpdateItem", ctx, "TestTable", mock.Anything, "SET a=1", "", mock.Anything, mock.Anything, "").Return(map[string]models.AttributeValue{}, nil)

	req := &models.ExecuteStatementRequest{
		Statement: "UPDATE TestTable SET a=1 WHERE pk='123'",
	}
	resp, err := svc.ExecuteStatement(ctx, req)
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	mockStore.AssertCalled(t, "UpdateItem", ctx, "TestTable", mock.Anything, "SET a=1", "", mock.Anything, mock.Anything, "")
}

func TestExecuteStatement_Delete(t *testing.T) {
	mockStore := new(MockStore)
	svc := NewTableService(mockStore)
	ctx := context.Background()

	mockStore.On("DeleteItem", ctx, "TestTable", mock.Anything, "", mock.Anything, mock.Anything, "").Return(map[string]models.AttributeValue{}, nil)

	req := &models.ExecuteStatementRequest{
		Statement: "DELETE FROM TestTable WHERE pk='123'",
	}
	resp, err := svc.ExecuteStatement(ctx, req)
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	mockStore.AssertCalled(t, "DeleteItem", ctx, "TestTable", mock.Anything, "", mock.Anything, mock.Anything, "")
}

func TestBatchExecuteStatement_Mixed(t *testing.T) {
	mockStore := new(MockStore)
	svc := NewTableService(mockStore)
	ctx := context.Background()

	// 1. SELECT -> GetItem
	mockStore.On("GetItem", ctx, "Table1", mock.Anything, "", mock.Anything, mock.Anything).Return(map[string]models.AttributeValue{}, nil)

	// 2. INSERT -> PutItem
	mockStore.On("PutItem", ctx, "Table2", mock.Anything, "", mock.Anything, mock.Anything, "").Return(map[string]models.AttributeValue{}, nil)

	req := &models.BatchExecuteStatementRequest{
		Statements: []models.BatchStatementRequest{
			{Statement: "SELECT * FROM Table1 WHERE pk='1'"},
			{Statement: "INSERT INTO Table2 VALUE ?", Parameters: []models.AttributeValue{{M: map[string]models.AttributeValue{"pk": {S: new(string)}}}}},
		},
	}

	resp, err := svc.BatchExecuteStatement(ctx, req)
	assert.NoError(t, err)
	assert.Len(t, resp.Responses, 2)
	assert.Nil(t, resp.Responses[0].Error)
	assert.Nil(t, resp.Responses[1].Error)
}
