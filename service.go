package goRPC

import (
	"go/ast"
	"log"
	"reflect"
	"sync/atomic"
)

type methodType struct {
	method    reflect.Method // 方法本身
	ArgType   reflect.Type   // 第一个参数的类型
	ReplyType reflect.Type   // 第二个参数的类型
	numCalls  uint64
}

func (m *methodType) NumCalls() uint64 {
	return atomic.LoadUint64(&m.numCalls)
}

func (m *methodType) newArgv() reflect.Value {
	var argv reflect.Value
	// 检查 m.ArgType 是否是一个指针类型
	if m.ArgType.Kind() == reflect.Ptr {
		argv = reflect.New(m.ArgType.Elem())
	} else {
		argv = reflect.New(m.ArgType).Elem()
	}
	return argv
}

func (m *methodType) newReplyv() reflect.Value {
	replyv := reflect.New(m.ReplyType.Elem())
	switch m.ReplyType.Elem().Kind() {
	case reflect.Map:
		replyv.Elem().Set(reflect.MakeMap(m.ReplyType.Elem()))
	case reflect.Slice:
		replyv.Elem().Set(reflect.MakeSlice(m.ReplyType.Elem(), 0, 0))
	}
	return replyv
}

type service struct {
	name   string                 // 结构体名称
	typ    reflect.Type           // 结构体类型
	ins    reflect.Value          // 结构体实例
	method map[string]*methodType // 存储映射的结构体的所有符合条件的方法
}

func (s *service) registerMethod() {
	s.method = make(map[string]*methodType)
	// 遍历反射中的所有方法
	for i := 0; i < s.typ.NumMethod(); i++ {
		// 获取第i个方法及反射类型
		method := s.typ.Method(i)
		mType := method.Type
		log.Println(mType)
		// 检查输入和输出数量
		if mType.NumIn() != 3 && mType.NumOut() != 1 {
			continue
		}
		// 检查输出是否为error
		if mType.Out(0) != reflect.TypeOf((*error)(nil)).Elem() {
			continue
		}
		// 从输入获取第二个和第三个参数的类型
		argType, replyType := mType.In(1), mType.In(2)
		if !isExportedOrBuiltinType(argType) || !isExportedOrBuiltinType(replyType) {
			continue
		}
		// 创建新的method类型
		s.method[method.Name] = &methodType{
			method:    method,
			ArgType:   argType,
			ReplyType: replyType,
		}
		log.Printf("Registered %v %v %v", s.name, method.Name, mType.NumIn())
	}
}

// 通过反射值调用方法
func (s *service) call(m *methodType, argv, replyv reflect.Value) error {
	atomic.AddUint64(&m.numCalls, 1)
	f := m.method.Func
	returnValues := f.Call([]reflect.Value{argv, replyv})
	if errInterface := returnValues[0].Interface().(error); errInterface != nil {
		return errInterface.(error)
	}
	return nil
}

func newService(ins interface{}) *service {
	s := new(service)
	// 入参映射为服务
	s.typ = reflect.TypeOf(ins)
	s.ins = reflect.ValueOf(ins)
	s.name = reflect.Indirect(s.ins).Type().Name()
	if !ast.IsExported(s.name) {
		log.Fatalf("%s is not exported", s.name)
	}
	s.registerMethod()
	return s
}

// 判断类型是否是导出或内建类型，只有导出和内建的类型才能被远程调用
func isExportedOrBuiltinType(t reflect.Type) bool {
	return ast.IsExported(t.Name()) || t.PkgPath() == ""
}
