package service

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tabeth/concretedb/models"
)

func TestTableService_TagResource(t *testing.T) {
	mockStore := new(MockStore)
	service := NewTableService(mockStore)
	ctx := context.Background()
	req := &models.TagResourceRequest{
		ResourceArn: "arn:aws:dynamodb:us-east-1:123:table/TagTable",
		Tags:        []models.Tag{{Key: "K", Value: "V"}},
	}

	t.Run("Success", func(t *testing.T) {
		mockStore.On("TagResource", ctx, req.ResourceArn, req.Tags).Return(nil).Once()
		err := service.TagResource(ctx, req)
		assert.NoError(t, err)
		mockStore.AssertExpectations(t)
	})

	t.Run("ValidationError_EmptyARN", func(t *testing.T) {
		err := service.TagResource(ctx, &models.TagResourceRequest{Tags: req.Tags})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "ResourceArn is required")
	})

	t.Run("ValidationError_EmptyTags", func(t *testing.T) {
		err := service.TagResource(ctx, &models.TagResourceRequest{ResourceArn: req.ResourceArn})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "Tags list cannot be empty")
	})

	t.Run("StoreError", func(t *testing.T) {
		mockStore.On("TagResource", ctx, req.ResourceArn, req.Tags).Return(errors.New("fail")).Once()
		err := service.TagResource(ctx, req)
		assert.Error(t, err)
	})
}

func TestTableService_UntagResource(t *testing.T) {
	mockStore := new(MockStore)
	service := NewTableService(mockStore)
	ctx := context.Background()
	req := &models.UntagResourceRequest{
		ResourceArn: "arn:aws:dynamodb:us-east-1:123:table/TagTable",
		TagKeys:     []string{"K"},
	}

	t.Run("Success", func(t *testing.T) {
		mockStore.On("UntagResource", ctx, req.ResourceArn, req.TagKeys).Return(nil).Once()
		err := service.UntagResource(ctx, req)
		assert.NoError(t, err)
		mockStore.AssertExpectations(t)
	})

	t.Run("ValidationError", func(t *testing.T) {
		err := service.UntagResource(ctx, &models.UntagResourceRequest{ResourceArn: ""})
		assert.Error(t, err)
	})

	t.Run("StoreError", func(t *testing.T) {
		mockStore.On("UntagResource", ctx, req.ResourceArn, req.TagKeys).Return(errors.New("fail")).Once()
		err := service.UntagResource(ctx, req)
		assert.Error(t, err)
	})
}

func TestTableService_ListTagsOfResource(t *testing.T) {
	mockStore := new(MockStore)
	service := NewTableService(mockStore)
	ctx := context.Background()
	req := &models.ListTagsOfResourceRequest{
		ResourceArn: "arn:aws:dynamodb:us-east-1:123:table/TagTable",
	}

	t.Run("Success", func(t *testing.T) {
		tags := []models.Tag{{Key: "K", Value: "V"}}
		mockStore.On("ListTagsOfResource", ctx, req.ResourceArn, "").Return(tags, "", nil).Once()
		resp, err := service.ListTagsOfResource(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, tags, resp.Tags)
		mockStore.AssertExpectations(t)
	})

	t.Run("ValidationError", func(t *testing.T) {
		_, err := service.ListTagsOfResource(ctx, &models.ListTagsOfResourceRequest{})
		assert.Error(t, err)
	})

	t.Run("StoreError", func(t *testing.T) {
		mockStore.On("ListTagsOfResource", ctx, req.ResourceArn, "").Return([]models.Tag{}, "", errors.New("fail")).Once()
		_, err := service.ListTagsOfResource(ctx, req)
		assert.Error(t, err)
	})
}
