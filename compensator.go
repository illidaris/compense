package compense

import (
	"context"
)

type ICompensator interface {
	Register(name string, exec Execute) error
	Execute(ctx context.Context, task *Task) (int64, IOutTasker, error)
	ExecuteAsync(ctx context.Context, task *Task) (int64, IOutTasker, error)
	BatchRetry(ctx context.Context, group string, limit int32) error
	TaskByID(ctx context.Context, id string) (IOutTasker, error)
}
