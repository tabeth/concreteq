package store

import (
	"context"
	"fmt"
	"strings"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"github.com/tabeth/concretedb/models"
	"github.com/tabeth/concretedb/store/internal/fdbadapter"
)

// Helper to extract TableName from ARN
// Format: arn:aws:dynamodb:region:account-id:table/TableName
func parseTableNameFromARN(arn string) (string, error) {
	parts := strings.Split(arn, "/")
	if len(parts) < 2 {
		return "", fmt.Errorf("invalid ARN format: %s", arn)
	}
	// Assuming the part after the last / is the table name, or "table/Name"
	// Standard ARN: ...:table/MyTable
	// OR ...:table/MyTable/stream/... (tags usually only on Table)

	// Let's look for "table/" in the string for safer extraction
	idx := strings.Index(arn, ":table/")
	if idx == -1 {
		return "", fmt.Errorf("invalid ARN: resource must be a table")
	}
	sub := arn[idx+7:] // everything after ":table/"

	// If there are more slashes, it might be a stream or index, but Tagging is usually table level
	// or we need to support sub-resources. Standard DynamoDB Tagging is on Table.
	// We will assume the segment until the next / or end is the TableName
	slashIdx := strings.Index(sub, "/")
	if slashIdx != -1 {
		return sub[:slashIdx], nil
	}
	return sub, nil
}

func (s *FoundationDBStore) TagResource(ctx context.Context, resourceArn string, tags []models.Tag) error {
	tableName, err := parseTableNameFromARN(resourceArn)
	if err != nil {
		return err
	}

	_, err = s.db.Transact(func(tr fdbadapter.FDBTransaction) (interface{}, error) {
		// 1. Check if table exists
		// We check using Exists on the directory provider
		exists, err := s.dir.Exists(tr, []string{"tables", tableName})
		if err != nil {
			return nil, err
		}
		if !exists {
			return nil, ErrTableNotFound
		}

		// 2. Write Tags
		// Path: tables/<TableName>/tags/<TagKey> = TagValue
		// We use the directory provider to open the nested path
		tagsDir, err := s.dir.CreateOrOpen(tr, []string{"tables", tableName, "tags"}, nil)
		if err != nil {
			return nil, err
		}

		for _, tag := range tags {
			key := tagsDir.Pack(tuple.Tuple{tag.Key})
			tr.Set(key, []byte(tag.Value))
		}

		return nil, nil
	})
	return err
}

func (s *FoundationDBStore) UntagResource(ctx context.Context, resourceArn string, tagKeys []string) error {
	tableName, err := parseTableNameFromARN(resourceArn)
	if err != nil {
		return err
	}

	_, err = s.db.Transact(func(tr fdbadapter.FDBTransaction) (interface{}, error) {
		exists, err := s.dir.Exists(tr, []string{"tables", tableName})
		if err != nil {
			return nil, err
		}
		if !exists {
			return nil, ErrTableNotFound
		}

		tagsDir, err := s.dir.CreateOrOpen(tr, []string{"tables", tableName, "tags"}, nil)
		if err != nil {
			return nil, err
		}

		for _, k := range tagKeys {
			key := tagsDir.Pack(tuple.Tuple{k})
			tr.Clear(key)
		}

		return nil, nil
	})
	return err
}

func (s *FoundationDBStore) ListTagsOfResource(ctx context.Context, resourceArn string, nextToken string) ([]models.Tag, string, error) {
	tableName, err := parseTableNameFromARN(resourceArn)
	if err != nil {
		return nil, "", err
	}

	tags, err := s.db.Transact(func(tr fdbadapter.FDBTransaction) (interface{}, error) {
		exists, err := s.dir.Exists(tr, []string{"tables", tableName})
		if err != nil {
			return nil, err
		}
		if !exists {
			return nil, ErrTableNotFound
		}

		tagsDir, err := s.dir.CreateOrOpen(tr, []string{"tables", tableName, "tags"}, nil)
		if err != nil {
			return nil, err
		}

		// Range scan the tags directory
		begin, end := tagsDir.FDBRangeKeys()
		r := fdb.KeyRange{Begin: begin, End: end}

		kvsResult := tr.GetRange(r, fdb.RangeOptions{Mode: fdb.StreamingModeWantAll})
		kvs, err := kvsResult.GetSliceWithError()
		if err != nil {
			return nil, err
		}

		var result []models.Tag
		for _, kv := range kvs {
			t, err := tagsDir.Unpack(kv.Key)
			if err != nil {
				continue
			}
			if len(t) < 1 {
				continue
			}
			tagKey := t[0].(string)
			tagValue := string(kv.Value)
			result = append(result, models.Tag{Key: tagKey, Value: tagValue})
		}
		return result, nil
	})

	if err != nil {
		return nil, "", err
	}

	return tags.([]models.Tag), "", nil
}
