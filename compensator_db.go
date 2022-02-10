package compense

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"math"
	"sync"
	"time"
	"xorm.io/xorm"
)

// NewDBCompensatorWithOption new a compensator base on db
func NewDBCompensatorWithOption(core *xorm.Engine, options ...Option) ICompensator {
	o := NewOption(options...)
	return &DBCompensator{
		core:   core,
		option: o,
	}
}

// NewDBCompensator new a compensator base on db
func NewDBCompensator(core *xorm.Engine, option *CompensationOption) ICompensator {
	return &DBCompensator{
		core:   core,
		option: option,
	}
}

// DBCompensator use db compensate
type DBCompensator struct {
	core   *xorm.Engine
	option *CompensationOption
}

// Logger use log
func (r *DBCompensator) Logger() ILogger {
	if r.option.log == nil {
		r.option.log = &defaultLogger{}
	}
	return r.option.log
}

// Register register execute func
func (r *DBCompensator) Register(key string, exec Execute) error {
	if r.option.funcMap == nil {
		r.option.funcMap = make(map[string]Execute)
	}
	if _, ok := r.option.funcMap[key]; ok {
		return fmt.Errorf("%s func has registed", key)
	}
	r.option.funcMap[key] = exec
	return nil
}

// BatchRetry batch retry func
func (r *DBCompensator) BatchRetry(ctx context.Context, group string, limit int32) error {
	tasks, err := r.batchLock(ctx, group, limit, r.option.offset)
	if err != nil {
		return err
	}
	if len(tasks) < 1 {
		return nil
	}
	var wg sync.WaitGroup
	for _, v := range tasks {
		wg.Add(1)
		task := v
		asyncCtx := r.option.newAsyncCtx(ctx)
		go func() {
			defer wg.Done()
			eachErr := r.execute(asyncCtx, &task)
			if eachErr != nil {
				r.Logger().ErrorCtxf(ctx, "%s %s %s", (&task).GetID(), (&task).GetName(), eachErr)
			}
		}()
	}
	wg.Wait()
	return nil
}

// Execute exec
func (r *DBCompensator) Execute(ctx context.Context, task *Task) (int64, IOutTasker, error) {
	rows, err := r.insertWithLock(ctx, task)
	if err != nil {
		return rows, task, err
	}
	err = r.execute(ctx, task)
	return rows, task, err
}

// ExecuteAsync exec async
func (r *DBCompensator) ExecuteAsync(ctx context.Context, task *Task) (int64, IOutTasker, error) {
	rows, err := r.insertWithLock(ctx, task)
	if err != nil {
		return rows, task, err
	}
	asyncCtx := r.option.newAsyncCtx(ctx)
	go func() {
		_ = r.execute(asyncCtx, task)
	}()
	return rows, task, err
}

// TaskByID only read record by id without locker
func (r *DBCompensator) TaskByID(ctx context.Context, id string) (IOutTasker, error) {
	session := r.core.NewSession()
	defer session.Close()
	session.Context(ctx)
	t := &Task{}
	b, err := session.ID(id).Get(t)
	if !b {
		return nil, err
	}
	return t, err
}

// ############################################################################################
// ####################################### private func #######################################
// ############################################################################################

// execute exec func
func (r *DBCompensator) execute(ctx context.Context, task *Task) (err error) {
	defer func() {
		rows, afterErr := r.after(ctx, task, err)
		r.notify(fmt.Sprintf("After Execute Rows %d Err%s", rows, afterErr))
	}()
	timeout := time.Millisecond * time.Duration(task.Timeout)
	timeoutCtx, cancel := context.WithTimeout(ctx, timeout)
	ch := make(chan struct{})
	errCh := make(chan error)
	defer cancel()
	go func(subCtx context.Context) {
		defer func() {
			if errRecover := recover(); errRecover != nil {
				errCh <- fmt.Errorf("panic %s", errRecover)
			}
			close(ch)
			close(errCh)
		}()
		if v, ok := r.option.funcMap[task.GetName()]; ok {
			err := v(subCtx, task)
			if err != nil {
				errCh <- err
			} else {
				ch <- struct{}{}
			}
			r.notify(fmt.Sprintf("%s[%s] Err %s ", task.GetName(), task.GetArgs(), err))
		}
	}(timeoutCtx)
	select {
	case <-ch:
		return nil
	case err := <-errCh:
		return err
	case <-timeoutCtx.Done():
		return fmt.Errorf("timeout %dms", timeout.Milliseconds())
	}
}

// insertWithLock insert record and locker by self
func (r *DBCompensator) insertWithLock(ctx context.Context, tasks ...*Task) (int64, error) {
	beans := make([]interface{}, 0)
	for _, task := range tasks {
		task.ID = uuid.NewString()
		task.TimeStamp = time.Now().Unix()
		task.Locker = task.ID
		task.LockExpire = task.TimeStamp + int64(math.Ceil(float64(task.Timeout)/500))
		beans = append(beans, task)
	}
	res, err := r.sessionExec(ctx, func(session *xorm.Session) (int64, error) {
		return session.Insert(beans...)
	})
	return res, err
}

// releaseLock release lock when lock_expire + offset < now(time_stamp), offset min is 180s
func (r *DBCompensator) releaseLock(ctx context.Context, offset time.Duration) (int64, error) {
	session := r.core.NewSession()
	session.Context(ctx)
	rows, err := session.
		Cols("locker", "lock_expire").
		Where("`lock_expire` >0 AND `lock_expire` < ?", time.Now().Add(-1*offset).Unix()).
		Update(&Task{})
	if err != nil {
		return 0, err
	}
	return rows, err
}

// batchLock batch lock record with lock time out, offset min is 180s
func (r *DBCompensator) batchLock(ctx context.Context, group string, limit int32, offset time.Duration) (tasks []Task, err error) {
	session := r.core.NewSession()
	session.Context(ctx)
	defer func() {
		if p := recover(); p != nil {
			_ = session.Rollback()
		} else if err != nil {
			_ = session.Rollback()
		}
		_ = session.Close()
	}()
	if err := session.Begin(); err != nil {
		return nil, err
	}
	locker := uuid.NewString()
	rows, err := session.
		Cols("locker", "retries", "lock_expire").
		Where("`group` = ? AND `lock_expire` < ?", group, time.Now().Add(-1*offset).Unix()).
		Asc("`time_stamp`").
		Limit(int(limit), 0).
		SetExpr("`retries`", "retries+1").
		SetExpr("`lock_expire`", fmt.Sprintf("ceil(timeout/500)+%d", time.Now().Unix())).
		Update(&Task{Locker: locker})
	if err != nil {
		return nil, err
	}
	tasks = make([]Task, 0)
	err = session.Where("locker=?", locker).Find(&tasks)
	if err != nil {
		return tasks, err
	}
	r.Logger().InfoCtxf(ctx, "[locker:%s]ExecWithTrans res:%d,err:%s", locker, rows, err)
	if err := session.Commit(); err != nil {
		return nil, err
	}
	return tasks, nil
}

func (r *DBCompensator) notify(message string) {
	if r.option.execCallBack != nil {
		r.option.execCallBack(message)
	}
}

func (r *DBCompensator) after(ctx context.Context, task ITasker, err error) (int64, error) {
	if err != nil {
		return r.recordLastError(ctx, task.GetID().(string), task.GetOwner(), err.Error())
	}
	return r.delete(ctx, task.GetID().(string), task.GetOwner())
}

func (r *DBCompensator) recordLastError(ctx context.Context, id, locker, msg string) (int64, error) {
	return r.sessionExec(ctx, func(session *xorm.Session) (int64, error) {
		return session.ID(id).Where("locker=?", locker).Cols("lock_expire", "last_exec_time", "last_error", "locker").Update(&Task{LastError: msg, LastExecTime: time.Now().Unix()})
	})
}

func (r *DBCompensator) delete(ctx context.Context, id, locker string) (int64, error) {
	return r.sessionExec(ctx, func(session *xorm.Session) (int64, error) {
		return session.ID(id).Where("locker=?", locker).Delete(&Task{})
	})
}

func (r *DBCompensator) sessionExec(ctx context.Context, exec func(*xorm.Session) (int64, error)) (int64, error) {
	session := r.core.NewSession()
	defer session.Close()
	session.Context(ctx)
	res, err := exec(session)
	r.Logger().DebugCtxf(ctx, "Execute res:%d,err:%s", res, err)
	return res, err
}
