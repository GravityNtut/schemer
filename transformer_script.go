package schemer

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/dop251/goja"
)

type TransformerScriptImpl struct {
}

func NewTransformerScript() *TransformerScriptImpl {
	return &TransformerScriptImpl{}
}

func (ts *TransformerScriptImpl) injectFuncs(vm *goja.Runtime) error {

	vm.Set("prepareRefs", func(call goja.FunctionCall) goja.Value {
		input := call.Argument(0).Export().(map[string]interface{})
		result := ts.PrepareRefs(input)
		return vm.ToValue(result)
	})

	vm.Set("scanStruct", func(call goja.FunctionCall) goja.Value {
		input := call.Argument(0).ToObject(vm)
		ts.ScanStruct(vm, input)
		return goja.Undefined()
	})

	return nil
}

func (ts *TransformerScriptImpl) PrepareRefs(source map[string]interface{}) map[string]interface{} {
	result := make(map[string]interface{})

	for sourceKey, value := range source {
		keyParts := strings.Split(sourceKey, ".")
		level := result

		for i := 0; i < len(keyParts); i++ {
			part := keyParts[i]

			// If we are at the last part, assign the value
			if i == len(keyParts)-1 {
				level[part] = value
			} else {
				// If the part does not exist, create a new map
				if _, ok := level[part]; !ok {
					level[part] = make(map[string]interface{})
				}

				// Move deeper into the nested map
				nextLevel, _ := level[part].(map[string]interface{})
				level = nextLevel
			}
		}
	}

	return result
}

func (ts *TransformerScriptImpl) ScanStruct(vm *goja.Runtime, obj *goja.Object) {

	// Get all keys of the object
	keys := obj.Keys()
	for _, key := range keys {
		value := obj.Get(key)

		if goja.IsUndefined(value) || goja.IsNaN(value) || goja.IsInfinity(value) {
			// Delete the key if the value is undefined
			obj.Delete(key)
		} else if goja.IsNull(value) {
			// Continue if the value is null
			continue
		} else if value.ExportType().Kind() == reflect.Slice {
			// If the value is an array, iterate over the elements and recursively call scanStruct
			arrayObj := value.ToObject(vm)
			length := int(arrayObj.Get("length").ToInteger())
			for i := 0; i < length; i++ {
				elem := arrayObj.Get(fmt.Sprint(i))

				if goja.IsUndefined(elem) || goja.IsNaN(elem) || goja.IsInfinity(elem) || goja.IsNull(elem) {
					// Set undefined elements to null or delete them as per requirement
					arrayObj.Set(fmt.Sprint(i), nil)
				} else {
					ts.ScanStruct(vm, elem.ToObject(vm))
				}
			}
		} else if value.ExportType().Kind() == reflect.Map {
			// If the value is an object, recursively call scanStruct
			ts.ScanStruct(vm, value.ToObject(vm))
		}
	}
}
