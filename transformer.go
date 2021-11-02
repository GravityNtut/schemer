package schemer

import (
	"fmt"
	"sync"
	"time"

	"github.com/BrobridgeOrg/schemer/types"
)

type Transformer struct {
	source  *Schema
	dest    *Schema
	script  string
	ctxPool sync.Pool
}

func NewTransformer(source *Schema, dest *Schema) *Transformer {

	t := &Transformer{
		source: source,
		dest:   dest,
		script: `function main() { return source; }`,
	}

	t.ctxPool.New = func() interface{} {
		return NewContext()
	}

	return t
}

func (t *Transformer) normalize(ctx *Context, schema *Schema, data map[string]interface{}) {

	for fieldName, def := range schema.Fields {

		val, ok := data[fieldName]
		if !ok {
			continue
		}

		if def.Type == TYPE_MAP {
			t.normalize(ctx, def.Definition, val.(map[string]interface{}))
			continue
		}

		if def.Type == TYPE_TIME {

			// Skip null
			if val == nil {
				continue
			}

			if def.Info.(*types.Time).Percision != types.TIME_PERCISION_MICROSECOND {
				v, _ := ctx.vm.New(ctx.vm.Get("Date").ToObject(ctx.vm), ctx.vm.ToValue(val.(time.Time).UnixNano()/1e6))
				data[fieldName] = v
			}
			continue
		}
	}
}

func (t *Transformer) initializeContext(ctx *Context, env map[string]interface{}, schema *Schema, data map[string]interface{}) error {

	if !ctx.IsReady() {
		err := ctx.PreloadScript(t.script)
		if err != nil {
			return err
		}
	}

	// Initializing environment varable
	ctx.vm.Set("env", env)

	// Native functions
	console := ctx.vm.NewObject()
	console.Set("log", func(args ...interface{}) {
		fmt.Println(args...)
	})
	ctx.vm.Set("console", console)

	// Normorlize data for JavaScript
	t.normalize(ctx, t.source, data)
	ctx.vm.Set("source", data)

	return nil
}

func (t *Transformer) Transform(env map[string]interface{}, input map[string]interface{}) ([]map[string]interface{}, error) {

	var data map[string]interface{} = input
	if t.source != nil {
		data = t.source.Normalize(input)
	}

	// Preparing context and runtime
	ctx := t.ctxPool.Get().(*Context)
	defer t.ctxPool.Put(ctx)

	err := t.initializeContext(ctx, env, t.source, data)
	if err != nil {
		return nil, err
	}

	//var fn func() map[string]interface{}
	var fn func() interface{}
	err = ctx.vm.ExportTo(ctx.vm.Get("main"), &fn)
	if err != nil {
		return nil, err
	}

	result := fn()
	if result == nil {
		return nil, nil
	}

	// Result is an object
	if v, ok := result.(map[string]interface{}); ok {

		var val map[string]interface{} = v
		if t.dest != nil {
			val = t.dest.Normalize(v)
		}

		// Normalized for destination schema then returning result
		return []map[string]interface{}{
			val,
		}, nil
	} else if v, ok := result.([]interface{}); ok {
		// Result is an array

		returnedValue := make([]map[string]interface{}, len(v))
		for i, d := range v {

			if v, ok := d.(map[string]interface{}); ok {

				var val map[string]interface{} = v
				if t.dest != nil {
					val = t.dest.Normalize(v)
				}

				returnedValue[i] = val
			}
		}

		return returnedValue, nil
	}

	return nil, nil
}

func (t *Transformer) SetScript(script string) {
	t.script = `
function run() {` + script + `}
function scanStruct(obj) {
	for (key in obj) {
		val = obj[key]
		if (val === undefined) {
			delete obj[key]
		} else if (val == null) {
			continue
		} else if (val instanceof Array) {
			scanStruct(val)
		} else if (typeof val === 'object') {
			scanStruct(val)
		}
	}
}
function main() {
	v = run()
	if (v === null)
		return null
	scanStruct(v)
	return v
}
`
}

func (t *Transformer) SetSourceSchema(schema *Schema) {
	t.source = schema
}

func (t *Transformer) SetDestinationSchema(schema *Schema) {
	t.dest = schema
}
