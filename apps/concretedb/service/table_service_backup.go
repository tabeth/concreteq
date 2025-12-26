package service

import (
	"context"

	"github.com/tabeth/concretedb/models"
)

func (s *TableService) CreateBackup(ctx context.Context, request *models.CreateBackupRequest) (*models.CreateBackupResponse, error) {
	if request.TableName == "" {
		return nil, models.New("ValidationException", "TableName is required")
	}
	if request.BackupName == "" {
		return nil, models.New("ValidationException", "BackupName is required")
	}

	// Delegate to store
	details, err := s.store.CreateBackup(ctx, request)
	if err != nil {
		return nil, err
	}
	return &models.CreateBackupResponse{BackupDetails: *details}, nil
}

func (s *TableService) DeleteBackup(ctx context.Context, request *models.DeleteBackupRequest) (*models.DeleteBackupResponse, error) {
	if request.BackupArn == "" {
		return nil, models.New("ValidationException", "BackupArn is required")
	}
	desc, err := s.store.DeleteBackup(ctx, request.BackupArn)
	if err != nil {
		return nil, err
	}
	return &models.DeleteBackupResponse{BackupDescription: *desc}, nil
}

func (s *TableService) ListBackups(ctx context.Context, request *models.ListBackupsRequest) (*models.ListBackupsResponse, error) {
	summaries, lastKey, err := s.store.ListBackups(ctx, request)
	if err != nil {
		return nil, err
	}

	resp := &models.ListBackupsResponse{
		BackupSummaries: summaries,
	}
	if lastKey != "" {
		resp.LastEvaluatedBackupArn = lastKey
	}
	return resp, nil
}

func (s *TableService) DescribeBackup(ctx context.Context, request *models.DescribeBackupRequest) (*models.DescribeBackupResponse, error) {
	if request.BackupArn == "" {
		return nil, models.New("ValidationException", "BackupArn is required")
	}
	desc, err := s.store.DescribeBackup(ctx, request.BackupArn)
	if err != nil {
		return nil, err
	}
	return &models.DescribeBackupResponse{BackupDescription: *desc}, nil
}

func (s *TableService) RestoreTableFromBackup(ctx context.Context, request *models.RestoreTableFromBackupRequest) (*models.RestoreTableFromBackupResponse, error) {
	if request.TargetTableName == "" {
		return nil, models.New("ValidationException", "TargetTableName is required")
	}
	if request.BackupArn == "" {
		return nil, models.New("ValidationException", "BackupArn is required")
	}
	desc, err := s.store.RestoreTableFromBackup(ctx, request)
	if err != nil {
		return nil, err
	}
	return &models.RestoreTableFromBackupResponse{TableDescription: *desc}, nil
}

// PITR
func (s *TableService) UpdateContinuousBackups(ctx context.Context, req *models.UpdateContinuousBackupsRequest) (*models.ContinuousBackupsDescription, error) {
	return s.store.UpdateContinuousBackups(ctx, req)
}

func (s *TableService) DescribeContinuousBackups(ctx context.Context, tableName string) (*models.ContinuousBackupsDescription, error) {
	return s.store.DescribeContinuousBackups(ctx, tableName)
}

func (s *TableService) RestoreTableToPointInTime(ctx context.Context, req *models.RestoreTableToPointInTimeRequest) (*models.TableDescription, error) {
	return s.store.RestoreTableToPointInTime(ctx, req)
}
