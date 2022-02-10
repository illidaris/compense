package compense

import (
	"context"
	"fmt"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"xorm.io/xorm"
)

func TestNewDBCompensator(t *testing.T) {
	mock := GetMock()
	option := NewOption(
		WithOffset(time.Second*180),
		WithFuncMap(map[string]Execute{}),
		WithExecCallBack(func(s string) {
			println(s)
		}),
		WithNewAsyncCtx(func(ctx context.Context) context.Context {
			return context.Background()
		}),
	)
	c := NewDBCompensator(mock.MockDB, option)
	if c == nil {
		t.Error("c is nil")
	}
}

func TestNewDBCompensatorWithOption(t *testing.T) {
	mock := GetMock()
	c := NewDBCompensatorWithOption(
		mock.MockDB,
		WithOffset(time.Second*180),
		WithFuncMap(map[string]Execute{}),
		WithExecCallBack(func(s string) {
			println(s)
		}),
		WithNewAsyncCtx(func(ctx context.Context) context.Context {
			return context.Background()
		}),
	)
	if c == nil {
		t.Error("c is nil")
	}
}

func TestDBCompensator_Register(t *testing.T) {
	c := MockCompensation()
	err := c.Register("a", func(ctx context.Context, tasker ITasker) error {
		return nil
	})
	if err != nil {
		t.Error(err)
	}
	err = c.Register("a", func(ctx context.Context, tasker ITasker) error {
		return nil
	})
	if err == nil {
		t.Error("register same name no error")
	}
}

func TestDBCompensator_Execute_Success(t *testing.T) {
	c := MockCompensation()
	ctx := context.Background()
	tA := &Task{
		Name:    "A",
		Body:    "随便吧",
		Source:  "A",
		Timeout: 5000,
	}
	_, result, _ := c.Execute(ctx, tA)
	res, err := c.TaskByID(ctx, result.GetID().(string))
	if err != nil {
		t.Error(err)
	}
	if res != nil {
		t.Error("res != nil")
	}
}

func TestDBCompensator_Execute_Error(t *testing.T) {
	c := MockCompensation()
	ctx := context.Background()
	tB := &Task{
		Name:    "B",
		Body:    "随便吧",
		Source:  "aA",
		Timeout: 5000,
	}
	_, tb, _ := c.Execute(ctx, tB)
	res, err := c.TaskByID(ctx, tb.GetID().(string))
	if err != nil {
		t.Error(err)
	}
	if res == nil {
		t.Error("res is nil")
	}
	compareTask(tB, res, t.Error)
}

func TestDBCompensator_Execute_Panic(t *testing.T) {
	c := MockCompensation()
	ctx := context.Background()
	tC := &Task{
		Name:    "C",
		Body:    "随便吧",
		Source:  "XC",
		Timeout: 5000,
	}
	_, tc, _ := c.Execute(ctx, tC)
	res, err := c.TaskByID(ctx, tc.GetID().(string))
	if err != nil {
		t.Error(err)
	}
	if res == nil {
		t.Error("res is nil")
	}
	compareTask(tC, res, t.Error)
}

func TestDBCompensator_Execute_Timeout(t *testing.T) {
	c := MockCompensation()
	ctx := context.Background()
	tD := &Task{
		Name:    "D",
		Body:    "随便吧",
		Source:  "XD",
		Timeout: 5000,
	}
	_, td, _ := c.Execute(ctx, tD)
	res, err := c.TaskByID(ctx, td.GetID().(string))
	if err != nil {
		t.Error(err)
	}
	if res == nil {
		t.Error("res is nil")
	}
	compareTask(tD, res, t.Error)
}

func TestDBCompensator_BatchRetry(t *testing.T) {
	c := mockCompensationWithOffset(time.Second * 1)
	ctx := context.Background()
	t1 := &Task{
		Name:    "C",
		Body:    "随便吧",
		Source:  "XD",
		Timeout: 5000,
	}
	t2 := &Task{
		Name:    "B",
		Body:    "B随便吧",
		Source:  "B",
		Timeout: 5000,
	}
	_, out1, _ := c.Execute(ctx, t1)
	_, out2, _ := c.Execute(ctx, t2)
	time.Sleep(time.Second * 5)
	_ = c.BatchRetry(ctx, "", 1000)
	time.Sleep(time.Second * 5)
	res, err := c.TaskByID(ctx, out1.GetID().(string))
	if err != nil {
		t.Error(err)
	}
	if res == nil {
		t.Error("res is nil")
	}
	if res.GetRetries() != 1 {
		t.Error("retries != 1")
	}
	compareTask(t1, res, t.Error)

	res, err = c.TaskByID(ctx, out2.GetID().(string))
	if err != nil {
		t.Error(err)
	}
	if res == nil {
		t.Error("res is nil")
	}
	if res.GetRetries() != 1 {
		t.Error("retries != 1")
	}
	compareTask(t2, res, t.Error)
}

func compareTask(source ITasker, result ITasker, callback func(args ...interface{})) {
	if source.GetName() != result.GetName() {
		callback(fmt.Sprintf("[%s] name %s %s", result.GetID(), source.GetName(), result.GetName()))
	}
	if source.GetArgs() != result.GetArgs() {
		callback(fmt.Sprintf("[%s]args %s %s", result.GetID(), source.GetArgs(), result.GetArgs()))
	}
}

func MockCompensation() ICompensator {
	return mockCompensationWithOffset(time.Second * 180)
}

func mockCompensationWithOffset(duration time.Duration) ICompensator {
	listener := make(chan string)
	go func() {
		defer close(listener)
		for v := range listener {
			println(v)
		}
	}()
	funcMap := make(map[string]Execute)
	funcMap["A"] = func(ctx context.Context, tasker ITasker) error {
		_, err := (&StructA{}).FuncA(tasker.GetArgs())
		return err
	}
	funcMap["B"] = func(ctx context.Context, tasker ITasker) error {
		_, err := FuncB(tasker.GetArgs())
		return err
	}
	funcMap["C"] = func(ctx context.Context, tasker ITasker) error {
		_, err := FuncC(tasker.GetArgs())
		return err
	}
	funcMap["D"] = func(ctx context.Context, tasker ITasker) error {
		_, err := FuncD(tasker.GetArgs())
		return err
	}
	mock := GetMock()
	option := NewOption(
		WithOffset(duration),
		WithFuncMap(funcMap),
		WithExecCallBack(func(s string) {
			println(s)
		}),
		WithNewAsyncCtx(func(ctx context.Context) context.Context {
			return context.Background()
		}),
	)
	c := NewDBCompensator(mock.MockDB, option)
	return c
}

type StructA struct {
	Name string
}

func (s *StructA) FuncA(args string) (string, error) {
	time.Sleep(time.Millisecond * 20)
	return "A " + args, nil
}

func FuncB(args string) (string, error) {
	time.Sleep(time.Millisecond * 50)
	return "B " + args, fmt.Errorf("%s exec failed", "B")
}

func FuncC(args string) (string, error) {
	time.Sleep(time.Millisecond * 10)
	panic("TMD出错了 " + args)
}

func FuncD(args string) (string, error) {
	time.Sleep(time.Second * 10)
	fmt.Println("10s 之后")
	panic(" 我不可能出现 " + args)
}

type TestMock struct {
	MockDB *xorm.Engine
}

var env *TestMock

func GetMock() *TestMock {
	if env == nil {
		e, err := xorm.NewEngine("mysql", "root:123456@tcp(192.168.97.224:3306)/test?charset=utf8mb4")
		if err != nil {
			panic(err)
		}
		e.ShowSQL(true)
		e.SetMaxIdleConns(10)
		e.SetMaxOpenConns(100)
		err = e.Sync2(new(Task))
		if err != nil {
			panic(err)
		}
		env = &TestMock{MockDB: e}
	}
	return env
}
