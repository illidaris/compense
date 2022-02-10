package compense

import (
	"encoding/json"
	"errors"
	"time"
)

var _ = ITasker(&Task{})

type Task struct {
	ID           string      `json:"id" xorm:"varchar(36) pk comment('唯一主键') 'id'"`                 //  样例
	TimeStamp    int64       `json:"timeStamp" xorm:"bigint comment('消息时间戳') 'time_stamp'"`         //  样例
	Expiration   int64       `json:"expiration" xorm:"bigint comment('生效时间戳') 'expiration'"`        //  样例
	NotBefore    int64       `json:"notBefore" xorm:"bigint comment('生效时间戳') 'not_before'"`         //  样例
	Timeout      int64       `json:"timeout" xorm:"bigint comment('执行超时ms') 'timeout'"`             //  样例
	AppID        string      `json:"appId" xorm:"varchar(32) comment('消息源AppId') 'app_id'"`         //  样例
	Group        string      `json:"group" xorm:"varchar(32) comment('消息分组') 'group'"`              //  样例
	Name         string      `json:"name" xorm:"varchar(32) comment('消息名') 'name'"`                 //  样例
	Body         string      `json:"body" xorm:"text comment('消息主体') 'body'"`                       //  样例
	Retries      int32       `json:"retries" xorm:"int comment('重试次数') 'retries'"`                  //  样例
	LockExpire   int64       `json:"lockExpire" xorm:"bigint comment('锁过期时间') 'lock_expire'"`       //  样例
	Source       string      `json:"source" xorm:"varchar(36) comment('来源') 'source'"`              //  样例
	Locker       string      `json:"locker" xorm:"varchar(36) comment('执行者') 'locker'"`             //  样例
	LastError    string      `json:"lastError" xorm:"varchar(255) comment('最后失败原因') 'last_error'"`  //  样例
	LastExecTime int64       `json:"lastExecTime" xorm:"bigint comment('最后执行时间') 'last_exec_time'"` //  样例
	Raw          interface{} `json:"-" xorm:"-"`                                                    //  样例
}

func (p *Task) TableName() string {
	return "compensation_task"
}

func (p *Task) GetID() interface{} {
	return p.ID
}

func (p *Task) GetName() string {
	return p.Name
}

func (p *Task) GetArgs() string {
	return p.Body
}

func (p *Task) GetParams() (map[string]interface{}, error) {
	params := make(map[string]interface{})
	err := json.Unmarshal([]byte(p.GetArgs()), &params)
	if err != nil {
		return params, err
	}
	if len(params) < 1 {
		return params, errors.New("params not enough")
	}
	return params, nil
}

func (p *Task) GetTimeout() time.Duration {
	now := time.Now().Unix()
	if p.LockExpire > now {
		return time.Second * time.Duration(p.LockExpire-now)
	}
	return 0
}

func (p *Task) GetDeadline() int64 {
	return p.LockExpire
}

func (p *Task) GetOwner() string {
	return p.Locker
}

func (p *Task) GetLastError() string {
	return p.LastError
}

func (p *Task) GetRetries() int32 {
	return p.Retries
}

func (p *Task) GetGroup() string {
	return p.Group
}
