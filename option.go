package compense

import (
	"context"
	"time"
)

// CompensationOption compensation option
type CompensationOption struct {
	log          ILogger
	offset       time.Duration // lockExpire margin
	funcMap      map[string]Execute
	execCallBack func(string)
	newAsyncCtx  func(ctx context.Context) context.Context
}

type Option interface {
	apply(option *CompensationOption)
}

// type optionFunc func
/**
 * @Description:
 * @param compensation
 */
type optionFunc func(option *CompensationOption)

// apply
/**
 * @Description:
 * @receiver f
 * @param c
 */
func (f optionFunc) apply(option *CompensationOption) {
	f(option)
}

// WithOffset
/**
 * @Description: suggest 180 seconds
 * @param v
 * @return Option
 */
func WithOffset(v time.Duration) Option {
	return optionFunc(func(option *CompensationOption) {
		option.offset = v
	})
}

// WithLogger
/**
 * @Description:
 * @param v
 * @return Option
 */
func WithLogger(v ILogger) Option {
	return optionFunc(func(option *CompensationOption) {
		option.log = v
	})
}

// WithFuncMap
/**
 * @Description:
 * @param v
 * @return Option
 */
func WithFuncMap(v map[string]Execute) Option {
	return optionFunc(func(option *CompensationOption) {
		option.funcMap = v
	})
}

// WithExecCallBack
/**
 * @Description:
 * @param v
 * @return Option
 */
func WithExecCallBack(v func(string)) Option {
	return optionFunc(func(option *CompensationOption) {
		option.execCallBack = v
	})
}

// WithNewAsyncCtx
/**
 * @Description:
 * @param v
 * @return Option
 */
func WithNewAsyncCtx(v func(ctx context.Context) context.Context) Option {
	return optionFunc(func(option *CompensationOption) {
		option.newAsyncCtx = v
	})
}

// NewOption
/**
 * @Description:
 * @param opts
 * @return CompensationOption
 */
func NewOption(opts ...Option) *CompensationOption {
	o := &CompensationOption{}
	for _, opt := range opts {
		opt.apply(o)
	}
	if o.log == nil {
		o.log = &defaultLogger{}
	}
	if o.funcMap == nil {
		o.funcMap = map[string]Execute{}
	}
	return o
}
