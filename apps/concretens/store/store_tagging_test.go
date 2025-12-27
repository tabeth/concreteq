package store

import (
	"context"
	"testing"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"github.com/kiroku-inc/kiroku-core/apps/concretens/models"
	"github.com/tabeth/kiroku-core/libs/fdb/fdbtest"
)

func TestStore_Tagging(t *testing.T) {
	fdbtest.SkipIfFDBUnavailable(t)

	s, err := NewStore(710)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	ctx := context.Background()

	// Setup Topic
	topic, err := s.CreateTopic(ctx, "tag-test-topic", nil)
	if err != nil {
		t.Fatalf("CreateTopic failed: %v", err)
	}

	// 1. TagResource
	tags := []models.Tag{
		{Key: "env", Value: "prod"},
		{Key: "owner", Value: "team-a"},
		{Key: "cost-center", Value: "123"},
	}
	err = s.TagResource(ctx, topic.TopicArn, tags)
	if err != nil {
		t.Errorf("TagResource failed: %v", err)
	}

	// 2. ListTagsForResource
	fetchedTags, err := s.ListTagsForResource(ctx, topic.TopicArn)
	if err != nil {
		t.Errorf("ListTagsForResource failed: %v", err)
	}
	if len(fetchedTags) != 3 {
		t.Errorf("Expected 3 tags, got %d", len(fetchedTags))
	}
	// Verify content
	tagMap := make(map[string]string)
	for _, tag := range fetchedTags {
		tagMap[tag.Key] = tag.Value
	}
	if tagMap["env"] != "prod" || tagMap["owner"] != "team-a" {
		t.Error("Tags content mismatch")
	}

	// 3. UntagResource
	untagKeys := []string{"cost-center", "owner"}
	err = s.UntagResource(ctx, topic.TopicArn, untagKeys)
	if err != nil {
		t.Errorf("UntagResource failed: %v", err)
	}

	// Verify removal
	fetchedTags, _ = s.ListTagsForResource(ctx, topic.TopicArn)
	if len(fetchedTags) != 1 {
		t.Errorf("Expected 1 tag after untag, got %d", len(fetchedTags))
	}
	if fetchedTags[0].Key != "env" {
		t.Errorf("Expected remaining tag 'env', got '%s'", fetchedTags[0].Key)
	}

	// 4. Invalid ARN
	err = s.TagResource(ctx, "invalid-arn", tags)
	if err == nil {
		t.Error("Expected error for invalid ARN")
	}

	// 5. Overwrite Tag
	newTags := []models.Tag{{Key: "env", Value: "staging"}}
	s.TagResource(ctx, topic.TopicArn, newTags)
	fetchedTags, _ = s.ListTagsForResource(ctx, topic.TopicArn)
	if len(fetchedTags) != 1 || fetchedTags[0].Value != "staging" {
		t.Errorf("Tag overwrite failed. Value: %s", fetchedTags[0].Value)
	}

	// 6. Validation Error on Untag
	err = s.UntagResource(ctx, "invalid-arn", untagKeys)
	if err == nil {
		t.Error("Expected error for Untag invalid ARN")
	}

	// 7. Validation Error on ListTags
	_, err = s.ListTagsForResource(ctx, "invalid-arn")
	if err == nil {
		t.Error("Expected error for ListTags invalid ARN")
	}

	// 8. Create with Tags (Test helper via CreateTopic) - NOT implemented yet in CreateTopic?
	// CreateTopic doesn't take tags.
	// But we can test TagResource on Subscription ARN
	sub, _ := s.Subscribe(ctx, &models.Subscription{
		TopicArn: topic.TopicArn,
		Protocol: "sqs",
		Endpoint: "arn:sqs:test",
	})

	err = s.TagResource(ctx, sub.SubscriptionArn, tags)
	if err != nil {
		t.Errorf("TagResource on Subscription failed: %v", err)
	}
	fetchedTagsSub, _ := s.ListTagsForResource(ctx, sub.SubscriptionArn)
	if len(fetchedTagsSub) != 3 {
		t.Errorf("Expected 3 tags on sub, got %d", len(fetchedTagsSub))
	}
}

func TestStore_Tagging_Corrupted(t *testing.T) {
	fdbtest.SkipIfFDBUnavailable(t)
	s, err := NewStore(710)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	ctx := context.Background()

	topicArn := "arn:concretens:topic:corrupt-tags"

	// Insert malformed key in tagDir
	// Key structure: tagDir + resourceArn + tagKey
	// Malformed: tagDir + resourceArn (missing tagKey) -> unpacking fails or len < 2
	s.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		key := s.tagDir.Pack(tuple.Tuple{topicArn})
		tr.Set(key, []byte("val"))
		return nil, nil
	})

	tags, err := s.ListTagsForResource(ctx, topicArn)
	if err != nil {
		t.Errorf("ListTagsForResource failed: %v", err)
	}
	if len(tags) != 0 {
		t.Errorf("Expected 0 tags (skipped corrupt), got %d", len(tags))
	}
}
