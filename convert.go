package schemer

import (
	"fmt"
	"math/big"
	"reflect"
	"strconv"
	"time"

	"github.com/BrobridgeOrg/schemer/types"
)

var (
	ErrInvalidType = fmt.Errorf("Invalid type")
)

func getStandardValue(data interface{}) interface{} {

	v := reflect.ValueOf(data)

	switch v.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return v.Int()
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return v.Uint()
	case reflect.Float32, reflect.Float64:
		return v.Float()
	}

	return data
}

func getValue(def *Definition, data interface{}) (interface{}, error) {

	if !def.NotNull && data == nil {
		return nil, ErrInvalidType
	}

	v := getStandardValue(data)

	// According to definition to convert value to what we want
	switch def.Type {
	case TYPE_INT64:
		return getIntegerValue(def, v)
	case TYPE_UINT64:
		return getUnsignedIntegerValue(def, v)
	case TYPE_FLOAT64:
		return getFloatValue(def, v)
	case TYPE_BOOLEAN:
		return getBoolValue(def, v)
	case TYPE_STRING:
		return getStringValue(def, v)
	case TYPE_TIME:
		t, err := def.Info.(*types.Time).GetValue(v)
		if err == types.ErrEmptyValue {
			return nil, ErrInvalidType
		}

		return t, nil
	case TYPE_BINARY:
		return getBinaryValue(def, v)
	case TYPE_MAP:
		return getMapValue(def, v)
	case TYPE_ARRAY:
		return getArrayValue(def, v)
	case TYPE_ANY:
		return v, nil
	}

	// Unknown type
	return v, nil
}

func float64ToString(d float64) string {
	// conver to  big.Float
	bf := big.NewFloat(d)
	// to string
	return bf.Text('f', -1)
}

func wrapParseInt64(d string) int64 {
	bi := new(big.Int)
	bi.SetString(d, 10)
	mask := new(big.Int).SetUint64(^uint64(0)) // 2^64 - 1
	bi.And(bi, mask)                           // 限制數值為 `uint64` 範圍
	return int64(bi.Int64())                   // 處理補數回繞
}

func wrapParseUint64(d string) uint64 {
	bi := new(big.Int)
	bi.SetString(d, 10)
	mask := new(big.Int).SetUint64(^uint64(0)) // 0xFFFFFFFFFFFFFFFF，即 `2^64 - 1`
	bi.And(bi, mask)                           // 限制數值為 `uint64` 範圍
	return bi.Uint64()
}

func getIntegerValue(def *Definition, data interface{}) (int64, error) {

	switch d := data.(type) {
	case int64:
		return d, nil
	case uint64:
		return int64(d), nil
	case string:
		result := wrapParseInt64(d)
		return result, nil
	case bool:
		if d {
			return int64(1), nil
		} else {
			return int64(0), nil
		}
	case float64:
		str := float64ToString(d)
		result := wrapParseInt64(str)
		return result, nil
	case time.Time:
		return d.Unix(), nil
	}

	return 0, nil
}

func getUnsignedIntegerValue(def *Definition, data interface{}) (uint64, error) {

	switch d := data.(type) {
	case int64:
		if d > 0 {
			return uint64(d), nil
		}

		return 0, ErrInvalidType
	case uint64:
		return d, nil
	case string:
		result := wrapParseUint64(d)
		return result, nil
	case bool:
		if d {
			return uint64(1), nil
		} else {
			return uint64(0), nil
		}
	case float64:
		str := float64ToString(d)
		result := wrapParseUint64(str)
		return result, nil
	case time.Time:
		return uint64(d.Unix()), nil
	}

	return 0, nil
}

func getFloatValue(def *Definition, data interface{}) (float64, error) {

	switch d := data.(type) {
	case int64:
		return float64(d), nil
	case uint64:
		return float64(d), nil
	case string:
		result, err := strconv.ParseFloat(d, 64)
		if err != nil {
			return 0, ErrInvalidType
		}

		return result, nil
	case bool:
		if d {
			return float64(1), nil
		} else {
			return float64(0), nil
		}
	case float64:
		return d, nil
	case time.Time:
		return float64(d.Unix()), nil
	}

	return 0, nil
}

func getBoolValue(def *Definition, data interface{}) (bool, error) {

	switch d := data.(type) {
	case int64:
		if d > 0 {
			return true, nil
		} else {
			return false, nil
		}
	case uint64:
		if d > 0 {
			return true, nil
		} else {
			return false, nil
		}
	case string:
		result, err := strconv.ParseBool(d)
		if err != nil {
			return false, ErrInvalidType
		}

		return result, nil
	case bool:
		return d, nil
	case float64:
		if d > 0 {
			return true, nil
		} else {
			return false, nil
		}
	case time.Time:
		return true, nil
	}

	return false, nil
}

func getStringValue(def *Definition, data interface{}) (string, error) {

	switch d := data.(type) {
	case string:
		return d, nil
	case int64:
		return fmt.Sprintf("%d", d), nil
	case uint64:
		return fmt.Sprintf("%d", d), nil
	case bool:
		return fmt.Sprintf("%t", d), nil
	case float64:
		return strconv.FormatFloat(d, 'f', -1, 64), nil
	case time.Time:
		return d.UTC().Format(time.RFC3339Nano), nil
	case map[string]interface{}:
		jsonData, _ := json.Marshal(d)
		return string(jsonData), ErrInvalidType
	case []interface{}:
		jsonData, _ := json.Marshal(d)
		return string(jsonData), ErrInvalidType
	default:
		return fmt.Sprintf("%v", d), nil
	}
}

func getBinaryValue(def *Definition, data interface{}) ([]byte, error) {

	switch d := data.(type) {
	case []byte:
		return d, nil
	case string:
		return []byte(d), nil
	case []interface{}:
		val := make([]byte, len(d))
		for i, v := range d {
			b, _ := getUnsignedIntegerValue(def, v)
			val[i] = byte(b)
		}

		return val, nil
	}

	return []byte(""), ErrInvalidType
}

func getMapValue(def *Definition, data interface{}) (map[string]interface{}, error) {

	switch d := data.(type) {
	case map[string]interface{}:
		return d, nil
	}

	return nil, ErrInvalidType
}

func getArrayValue(def *Definition, data interface{}) (interface{}, error) {

	if data == nil {
		return nil, nil
	}

	// prevent to use reflection if possible
	switch d := data.(type) {
	case []interface{}:
		value := make([]interface{}, len(d))
		for i, v := range d {
			val, err := getValue(def.Subtype, v)
			if err != nil {
				return nil, ErrInvalidType
			}

			value[i] = val
		}
		return value, nil
	}

	// Not an array
	v := reflect.ValueOf(data)
	if v.Kind() != reflect.Array && v.Kind() != reflect.Slice {
		return nil, ErrInvalidType
	}

	value := make([]interface{}, v.Len())
	for i := 0; i < v.Len(); i++ {

		// Get value of element
		val, err := getValue(def.Subtype, v.Index(i).Interface())
		if err != nil {
			return nil, ErrInvalidType
		}

		value[i] = val
	}

	return value, nil
}

/*
func convert(sourceDef *Definition, destDef *Definition, data interface{}) interface{} {

	srcData := getValue(sourceDef, data)

	return getValue(destDef, srcData)
}
*/
