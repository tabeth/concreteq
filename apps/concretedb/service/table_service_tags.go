package service

import (
	"context"

	"github.com/tabeth/concretedb/models"
)

// TagResource adds tags to a resource (Table).
func (s *TableService) TagResource(ctx context.Context, req *models.TagResourceRequest) error {
	if req.ResourceArn == "" {
		return models.New("ValidationException", "ResourceArn is required")
	}
	if len(req.Tags) == 0 {
		return models.New("ValidationException", "Tags list cannot be empty")
	}

	err := s.store.TagResource(ctx, req.ResourceArn, req.Tags)
	if err != nil {
		// Passthrough store errors (e.g. ErrTableNotFound -> ResourceNotFoundException ideally)
		// Assuming Store methods return generic errors we should wrap or specific errors we should map.
		// foundationdb_tags.go returns generic errors or ErrTableNotFound
		return err
	}
	return nil
}

// UntagResource removes tags from a resource.
func (s *TableService) UntagResource(ctx context.Context, req *models.UntagResourceRequest) error {
	if req.ResourceArn == "" {
		return models.New("ValidationException", "ResourceArn is required")
	}
	if len(req.TagKeys) == 0 {
		return models.New("ValidationException", "TagKeys list cannot be empty")
	}

	err := s.store.UntagResource(ctx, req.ResourceArn, req.TagKeys)
	if err != nil {
		return err
	}
	return nil
}

// ListTagsOfResource lists tags for a resource.
func (s *TableService) ListTagsOfResource(ctx context.Context, req *models.ListTagsOfResourceRequest) (*models.ListTagsOfResourceResponse, error) {
	if req.ResourceArn == "" {
		return nil, models.New("ValidationException", "ResourceArn is required")
	}

	tags, nextToken, err := s.store.ListTagsOfResource(ctx, req.ResourceArn, req.NextToken)
	if err != nil {
		return nil, err
	}

	return &models.ListTagsOfResourceResponse{
		Tags:      tags,
		NextToken: nextToken,
	}, nil
}
