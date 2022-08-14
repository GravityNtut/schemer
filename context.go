package schemer

import (
	"github.com/dop251/goja"
)

type Context struct {
	vm     *goja.Runtime
	output map[string]interface{}
	ready  bool
}

func NewContext() *Context {
	return &Context{
		vm:     goja.New(),
		output: make(map[string]interface{}),
	}
}

func (ctx *Context) PreloadScript(p *goja.Program) error {
	_, err := ctx.vm.RunProgram(p)
	if err != nil {
		return err
	}

	ctx.ready = true

	return nil
}

func (ctx *Context) IsReady() bool {
	return ctx.ready
}
