package compense

import "xorm.io/xorm"

var global ICompensator

func Global() ICompensator {
	return global
}

func ByDb(core *xorm.Engine, opts ...Option) {
	global = NewDBCompensatorWithOption(core, opts...)
}
