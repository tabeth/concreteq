package store

import (
	"context"
	"encoding/json"

	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"github.com/tabeth/concretedb/models"
	"github.com/tabeth/concretedb/store/internal/fdbadapter"
)

// UpdateTimeToLive updates the TTL configuration for a table.
func (s *FoundationDBStore) UpdateTimeToLive(ctx context.Context, request *models.UpdateTimeToLiveRequest) (*models.TimeToLiveSpecification, error) {
	val, err := s.db.Transact(func(tr fdbadapter.FDBTransaction) (interface{}, error) {
		// 1. Get Table Metadata
		table, err := s.getTableInternal(tr, request.TableName)
		if err != nil {
			return nil, err
		}
		if table == nil {
			return nil, ErrTableNotFound
		}

		// 2. Update TTL Description
		if table.TimeToLiveDescription == nil {
			table.TimeToLiveDescription = &models.TimeToLiveDescription{
				TimeToLiveStatus: "DISABLED",
			}
		}

		// Simplified state transition logic for now
		if request.TimeToLiveSpecification.Enabled {
			table.TimeToLiveDescription.TimeToLiveStatus = "ENABLED"
			table.TimeToLiveDescription.AttributeName = request.TimeToLiveSpecification.AttributeName
		} else {
			table.TimeToLiveDescription.TimeToLiveStatus = "DISABLED"
			// Keep the attribute name even if disabled, similar to DynamoDB behavior often retaining context
		}

		// 3. Save updated metadata
		tableBytes, err := json.Marshal(table)
		if err != nil {
			return nil, err
		}

		tableDir, err := s.dir.Open(tr, []string{"tables", table.TableName}, nil)
		if err != nil {
			return nil, err
		}
		tr.Set(tableDir.Pack(tuple.Tuple{"metadata"}), tableBytes)

		return &request.TimeToLiveSpecification, nil
	})

	if err != nil {
		return nil, err
	}
	return val.(*models.TimeToLiveSpecification), nil
}

// DescribeTimeToLive retrieves the TTL configuration for a table.
func (s *FoundationDBStore) DescribeTimeToLive(ctx context.Context, tableName string) (*models.TimeToLiveDescription, error) {
	val, err := s.db.ReadTransact(func(rtr fdbadapter.FDBReadTransaction) (interface{}, error) {
		table, err := s.getTableInternal(rtr, tableName)
		if err != nil {
			return nil, err
		}
		if table == nil {
			return nil, ErrTableNotFound
		}

		if table.TimeToLiveDescription == nil {
			// Default if not set
			return &models.TimeToLiveDescription{
				TimeToLiveStatus: "DISABLED",
			}, nil
		}

		return table.TimeToLiveDescription, nil
	})

	if err != nil {
		return nil, err
	}
	return val.(*models.TimeToLiveDescription), nil
}

// Helper to get table internal (reused from foundationdb_store.go but since this is a new file we can't access private methods if they are in another file in the same package?
// No, Go allows access to private methods in the same package.
// However, I need to make sure getTableInternal is available or duplicate it.
// Checking foundationdb_store.go again... getTableInternal IS NOT exported, so I can use it if I put this code in a file in the same package.)
