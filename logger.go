package compense

import (
	"context"
	"fmt"
)

type ILogger interface {
	DebugCtxf(ctx context.Context, format string, args ...interface{})
	InfoCtxf(ctx context.Context, format string, args ...interface{})
	WarnCtxf(ctx context.Context, format string, args ...interface{})
	ErrorCtxf(ctx context.Context, format string, args ...interface{})
}

type defaultLogger struct{}

func (logger *defaultLogger) DebugCtxf(ctx context.Context, format string, args ...interface{}) {
	fmt.Printf(format+"/n", args...)
}

func (logger *defaultLogger) InfoCtxf(ctx context.Context, format string, args ...interface{}) {
	fmt.Printf(format+"/n", args...)
}

func (logger *defaultLogger) WarnCtxf(ctx context.Context, format string, args ...interface{}) {
	fmt.Printf(format+"/n", args...)
}

func (logger *defaultLogger) ErrorCtxf(ctx context.Context, format string, args ...interface{}) {
	fmt.Printf(format+"/n", args...)
}
