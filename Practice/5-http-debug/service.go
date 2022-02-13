package gorpc

import (
	"go/ast"
	"log"
	"reflect"
	"sync/atomic"
)

// 方法实例
type methodType struct {
	// 方法本身
	method reflect.Method
	// 参数:调用方法参数类型
	ArgType reflect.Type
	// 参数:RPC回复参数类型
	ReplyType reflect.Type
	// RPC调用序号
	numCalls uint64
}

// NumCalls 随机生成
func (m *methodType) NumCalls() uint64 {
	return atomic.LoadUint64(&m.numCalls)
}

// newArgv 创建对应类型实例
func (m *methodType) newArgv() reflect.Value {
	var argv reflect.Value

	if m.ArgType.Kind() == reflect.Ptr {
		// arg为指针类型
		argv = reflect.New(m.ArgType.Elem())
	} else {
		// arg为值类型
		argv = reflect.New(m.ArgType).Elem()
	}
	return argv
}

// newReplyv 创建对应类型实例
func (m *methodType) newReplyv() reflect.Value {
	//TODO reply为指针类型
	replyv := reflect.New(m.ReplyType.Elem())
	switch m.ReplyType.Elem().Kind() {
	case reflect.Map:
		replyv.Elem().Set(reflect.MakeMap(m.ReplyType.Elem()))
	case reflect.Slice:
		replyv.Elem().Set(reflect.MakeSlice(m.ReplyType.Elem(), 0, 0))
	}
	return replyv
}

// 服务实例
type service struct {
	// 名字
	name string
	// 类型
	typ reflect.Type
	// 实例本身 -> RPC调用时 用作第一个参数
	rcvr reflect.Value
	// 存储符合条件的方法
	method map[string]*methodType
}

// newService 构造函数
func newService(rcvr interface{}) *service {
	s := new(service)
	s.rcvr = reflect.ValueOf(rcvr)
	s.name = reflect.Indirect(s.rcvr).Type().Name()
	s.typ = reflect.TypeOf(rcvr)
	if !ast.IsExported(s.name) {
		log.Fatalf("rpc server: %s is not a valid service name", s.name)
	}
	s.registerMethods()
	return s
}

// registerMethods 查找符合条件的方法
func (s *service) registerMethods() {
	s.method = make(map[string]*methodType)
	// s.typ.NumMethod() -> 方法中可访问方法的数量(可导出）
	for i := 0; i < s.typ.NumMethod(); i++ {
		method := s.typ.Method(i)
		mType := method.Type
		// 筛选条件: 入参为3 出参为1
		if mType.NumIn() != 3 || mType.NumOut() != 1 {
			continue
		}
		if mType.Out(0) != reflect.TypeOf((*error)(nil)).Elem() {
			continue
		}
		argType, replyType := mType.In(1), mType.In(2)
		if !isExportedOrBuiltinType(argType) || !isExportedOrBuiltinType(replyType) {
			continue
		}
		s.method[method.Name] = &methodType{
			method:    method,
			ArgType:   argType,
			ReplyType: replyType,
		}
		log.Printf("rpc server: register %s.%s\n", s.name, method.Name)
	}
}

// call 通过反射值调用方法
func (s *service) call(m *methodType, argv, replyv reflect.Value) error {
	atomic.AddUint64(&m.numCalls, 1)
	f := m.method.Func
	// TODO 通过反射 根据入参 获得返回值
	returnValues := f.Call([]reflect.Value{s.rcvr, argv, replyv})
	if errInter := returnValues[0].Interface(); errInter != nil {
		return errInter.(error)
	}
	return nil
}

// 判断该是否为导出方法
func isExportedOrBuiltinType(t reflect.Type) bool {
	return ast.IsExported(t.Name()) || t.PkgPath() == ""
}
