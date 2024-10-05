package tests

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/BrobridgeOrg/schemer"
	"github.com/stretchr/testify/assert"
)

var mapSchema1 = `
{
   "id":{
      "type":"uint"
   },
   "map_col":{
      "type":"map"
   }
}
`

var mapSchema2 = `
{
   "id":{
      "type":"uint"
   },
   "map_col":{
      "type":"map",
      "fields":{
         "nested_time":{
            "type":"time",
            "precision":"milisecond"
         }
      }
   }
}
`

var mapSchema3 = `
{
   "id":{
      "type":"uint"
   },
   "map_col":{
      "type":"map",
      "fields":{
         "string_col":{
            "type":"string"
         },
         "binary_col":{
            "type":"binary"
         },
         "int_col":{
            "type":"int"
         },
         "uint_col":{
            "type":"uint"
         },
         "float_col":{
            "type":"float"
         },
         "bool_col":{
            "type":"bool"
         },
         "any_col":{
            "type":"any"
         }
      }
   }
}
`

func normalize_map_schema1(s *schemer.Schema, id, map_col string) (map[string]interface{}, error) {
	jsonInput := fmt.Sprintf(`
	{
		"id":             %s,
		"map_col":        %s
	}`, id, map_col)
	var rawData map[string]interface{}
	err := json.Unmarshal([]byte(jsonInput), &rawData)
	if err != nil {
		return nil, err
	}
	return s.Normalize(rawData), nil
}

func normalize_map_schema2(s *schemer.Schema, id, nested_time string) (map[string]interface{}, error) {
	jsonInput := fmt.Sprintf(`
	{
		"id":              %s,
		"map_col": {
			"nested_time": %s
		}
	}`, id, nested_time)
	var rawData map[string]interface{}
	err := json.Unmarshal([]byte(jsonInput), &rawData)
	if err != nil {
		return nil, err
	}
	return s.Normalize(rawData), nil
}

func normalize_map_schema3(s *schemer.Schema, id, string_col, binary_col, int_col, uint_col, float_col, bool_col, any_col string) (map[string]interface{}, error) {
	jsonInput := fmt.Sprintf(`
	{
		"id":             %s,
		"map_col": {
			"string_col": %s,
			"binary_col": %s,
			"int_col":    %s,
			"uint_col":   %s,
			"float_col":  %s,
			"bool_col":   %s,
			"any_col":    %s
		}
	}`, id, string_col, binary_col, int_col, uint_col, float_col, bool_col, any_col)
	var rawData map[string]interface{}
	err := json.Unmarshal([]byte(jsonInput), &rawData)
	if err != nil {
		return nil, err
	}
	return s.Normalize(rawData), nil
}

func transformAndAssert(t *testing.T, transformer *schemer.Transformer, source map[string]interface{}) (map[string]interface{}, error) {
	returnedValue, err := transformer.Transform(nil, source)
	if !assert.Nil(t, err) {
		return nil, fmt.Errorf("transform failed: %v", err)
	}

	if !assert.Len(t, returnedValue, 1) {
		return nil, fmt.Errorf("return length not match")
	}
	result := returnedValue[0]
	return result, nil
}

func Test_Map_Fail(t *testing.T) {
	testSourceSchema := schemer.NewSchema()
	err := schemer.UnmarshalJSON([]byte(mapSchema3), testSourceSchema)
	if err != nil {
		t.Error(err)
		return
	}
	testDestSchema := schemer.NewSchema()
	err = schemer.UnmarshalJSON([]byte(mapSchema3), testDestSchema)
	if err != nil {
		t.Error(err)
		return
	}
	transformer := schemer.NewTransformer(testSourceSchema, testDestSchema)
	transformer.SetScript(`return source`)

	source, err := normalize_map_schema3(testSourceSchema, `1`, `5`, `"abc"`, `""`, `""`, `""`, `""`, `""`)
	if err != nil {
		t.Error(err)
		return
	}
	result, err := transformAndAssert(t, transformer, source)
	if err != nil {
		t.Error(err)
		return
	}
	assert.Equal(t, uint64(1), result["id"])
	map_col := result["map_col"].(map[string]interface{})
	assert.Equal(t, "5", map_col["string_col"])
	assert.Equal(t, []byte{0x61, 0x62, 0x63}, map_col["binary_col"])
	assert.Equal(t, int64(0), map_col["int_col"])
	assert.Equal(t, uint64(0), map_col["uint_col"])
	assert.Equal(t, float64(0), map_col["float_col"])
	assert.Equal(t, false, map_col["bool_col"])
	assert.Equal(t, "", map_col["any_col"])

	source, err = normalize_map_schema3(testSourceSchema, `2`, `5`, `"10102"`, `"!@#$%^&*()_+{}:<>?~-=[]\;',./"`, `"!@#$%^&*()_+{}:<>?~-=[]\;',./"`, `"!@#$%^&*()_+{}:<>?~-=[]\;',./"`, `"!@#$%^&*()_+{}:<>?~-=[]\;',./"`, `" "`)
	if err != nil {
		t.Error(err)
	}
	result, err = transformAndAssert(t, transformer, source)
	if err != nil {
		t.Error(err)
	}
	assert.Equal(t, uint64(2), result["id"])
	map_col = result["map_col"].(map[string]interface{})
	assert.Equal(t, "5", map_col["string_col"])
	assert.Equal(t, int64(0), map_col["int_col"])
	assert.Equal(t, uint64(0), map_col["uint_col"])
	assert.Equal(t, float64(0), map_col["float_col"])
	assert.Equal(t, false, map_col["bool_col"])
	assert.Equal(t, " ", map_col["any_col"])
}

func Test_Map_Success(t *testing.T) {
	testSourceSchema := schemer.NewSchema()
	err := schemer.UnmarshalJSON([]byte(mapSchema3), testSourceSchema)
	if err != nil {
		t.Error(err)
		return
	}
	testDestSchema := schemer.NewSchema()
	err = schemer.UnmarshalJSON([]byte(mapSchema3), testDestSchema)
	if err != nil {
		t.Error(err)
		return
	}
	transformer := schemer.NewTransformer(testSourceSchema, testDestSchema)
	transformer.SetScript(`return source`)

	source, err := normalize_map_schema3(testSourceSchema, `1`, `""`, `""`, `5`, `5`, `5`, `0`, `""`)
	if err != nil {
		t.Error(err)
		return
	}
	result, err := transformAndAssert(t, transformer, source)
	if err != nil {
		t.Error(err)
		return
	}
	assert.Equal(t, uint64(1), result["id"])
	map_col := result["map_col"].(map[string]interface{})
	assert.Equal(t, "", map_col["string_col"])
	assert.Equal(t, []byte{}, map_col["binary_col"])
	assert.Equal(t, int64(5), map_col["int_col"])
	assert.Equal(t, uint64(5), map_col["uint_col"])
	assert.Equal(t, float64(5), map_col["float_col"])
	assert.Equal(t, false, map_col["bool_col"])
	assert.Equal(t, "", map_col["any_col"])

	source, err = normalize_map_schema3(testSourceSchema, `2`, `" "`, `" "`, `0`, `0`, `1.23`, `1`, `" "`)
	if err != nil {
		t.Error(err)
	}
	result, err = transformAndAssert(t, transformer, source)
	if err != nil {
		t.Error(err)
	}
	assert.Equal(t, uint64(2), result["id"])
	map_col = result["map_col"].(map[string]interface{})
	assert.Equal(t, " ", map_col["string_col"])
	assert.Equal(t, []byte{0x20}, map_col["binary_col"])
	assert.Equal(t, int64(0), map_col["int_col"])
	assert.Equal(t, uint64(0), map_col["uint_col"])
	assert.Equal(t, float64(1.23), map_col["float_col"])
	assert.Equal(t, true, map_col["bool_col"])
	assert.Equal(t, " ", map_col["any_col"])

	source, err = normalize_map_schema3(testSourceSchema, `3`, `"abc"`, `"0"`, `-1`, `5`, `-1.23`, `"false"`, `"abc"`)
	if err != nil {
		t.Error(err)
	}
	result, err = transformAndAssert(t, transformer, source)
	if err != nil {
		t.Error(err)
	}
	assert.Equal(t, uint64(3), result["id"])
	map_col = result["map_col"].(map[string]interface{})
	assert.Equal(t, "abc", map_col["string_col"])
	assert.EqualValues(t, []byte{0x30}, map_col["binary_col"])
	assert.Equal(t, int64(-1), map_col["int_col"])
	assert.Equal(t, uint64(5), map_col["uint_col"])
	assert.Equal(t, float64(-1.23), map_col["float_col"])
	assert.Equal(t, false, map_col["bool_col"])
	assert.Equal(t, "abc", map_col["any_col"])

	source, err = normalize_map_schema3(testSourceSchema, `4`, `"中文"`, `"0"`, `5`, `0`, `-1.234567111111111`, `"true"`, `"中文"`)
	if err != nil {
		t.Error(err)
	}
	result, err = transformAndAssert(t, transformer, source)
	if err != nil {
		t.Error(err)
	}
	assert.Equal(t, uint64(4), result["id"])
	map_col = result["map_col"].(map[string]interface{})
	assert.Equal(t, "中文", map_col["string_col"])
	assert.EqualValues(t, []byte{0x30}, map_col["binary_col"])
	assert.Equal(t, int64(5), map_col["int_col"])
	assert.Equal(t, uint64(0), map_col["uint_col"])
	assert.Equal(t, float64(-1.234567111111111), map_col["float_col"])
	assert.Equal(t, true, map_col["bool_col"])
	assert.Equal(t, "中文", map_col["any_col"])
}
