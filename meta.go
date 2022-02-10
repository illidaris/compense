package compense

import (
	"context"
)

type IExecutor interface {
	Registry(name string, exec Execute)
}

type Execute func(ctx context.Context, tasker ITasker) error

type ITasker interface {
	GetID() interface{}
	GetGroup() string
	GetName() string
	GetArgs() string
	GetParams() (map[string]interface{}, error)
	GetDeadline() int64
	GetOwner() string
}

type IOutTasker interface {
	ITasker
	GetLastError() string
	GetRetries() int32
}
