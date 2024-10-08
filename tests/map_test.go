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

var (
	// SPECIAL_CHAR = `"!@#$%^&*()_+{}:<>?~-=[]\;',./"`
	// SPECIAL_CHAR_EXPECTED_OUTPUT = `!@#$%^&*()_+{}:<>?~-=[]\;',./`
	SPECIAL_CHAR                 = `"!@#$%^&*()_+{}:<>?~-=[]',./"`
	SPECIAL_CHAR_EXPECTED_OUTPUT = `!@#$%^&*()_+{}:<>?~-=[]',./`
	LARGE_STRING_EXPECTED_OUTPUT string
	LARGE_BYTE_EXPECTED_OUTPUT   []byte
	LARGE_STRING                 string
	LARGE_BYTE                   string
)

func init() {
	LARGE_STRING_EXPECTED_OUTPUT = ""
	LARGE_BYTE_EXPECTED_OUTPUT := []byte{}
	LARGE_BYTE = ""
	for i := 0; i < 32768; i++ {
		LARGE_STRING_EXPECTED_OUTPUT += "a"
		LARGE_BYTE_EXPECTED_OUTPUT = append(LARGE_BYTE_EXPECTED_OUTPUT, 0x30)
		LARGE_BYTE += "0"
	}
	LARGE_STRING = fmt.Sprintf(`"%s"`, LARGE_STRING)
	LARGE_BYTE = fmt.Sprintf(`"%s"`, LARGE_BYTE)
}

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

func TestMapTransformErrorHandle(t *testing.T) {
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

	maptest1 := map3Input{`1`, `5`, `"abc"`, `""`, `""`, `""`, `""`, `""`}
	maptest1Expected := map3Expected{1, "5", []byte{0x61, 0x62, 0x63}, 0, 0, 0, false, ""}
	transformTest(t, testSourceSchema, transformer, maptest1, maptest1Expected)

	maptest2 := map3Input{`2`, `5`, `"10102"`, SPECIAL_CHAR, SPECIAL_CHAR, SPECIAL_CHAR, SPECIAL_CHAR, `""`}
	maptest2Expected := map3Expected{2, "5", []byte{0x31, 0x30, 0x31, 0x30, 0x32}, 0, 0, 0, false, ""}
	transformTest(t, testSourceSchema, transformer, maptest2, maptest2Expected)

	// maptest3 := map3Input{`3`, `5`, SPECIAL_CHAR, `"abc"`, `"abc"`, `"abc"`, `"abc"`, `""`}
	// maptest3Expected := map3Expected{3, "5", []byte{}, 0, 0, 0, false, ""}
	// transformTest(t, testSourceSchema, transformer, maptest3, maptest3Expected)

}

func TestMapSuccessTransform(t *testing.T) {
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

	maptest1 := map3Input{`1`, `""`, `""`, `5`, `5`, `5`, `0`, `""`}
	maptest1Expected := map3Expected{1, "", []byte{}, 5, 5, 5, false, ""}
	transformTest(t, testSourceSchema, transformer, maptest1, maptest1Expected)

	maptest2 := map3Input{`2`, `" "`, `" "`, `0`, `0`, `1.23`, `1`, `" "`}
	maptest2Expected := map3Expected{2, " ", []byte{0x20}, 0, 0, 1.23, true, " "}
	transformTest(t, testSourceSchema, transformer, maptest2, maptest2Expected)

	// maptest3 := map3Input{`3`, `"abc"`, LARGE_BYTE, `-1`, `5`, `-1.23`, `"false"`, `"abc"`}
	// maptest3Expected := map3Expected{3, "abc", LARGE_BYTE_EXPECTED_OUTPUT, -1, 5, -1.23, false, "abc"}
	// transformTest(t, testSourceSchema, transformer, maptest3, maptest3Expected)

	maptest4 := map3Input{`4`, `"中文"`, `"0"`, `5`, `0`, `-1.234567111111111`, `"true"`, `"中文"`}
	maptest4Expected := map3Expected{4, "中文", []byte{0x30}, 5, 0, -1.234567111111111, true, "中文"}
	transformTest(t, testSourceSchema, transformer, maptest4, maptest4Expected)

	maptest5 := map3Input{`5`, SPECIAL_CHAR, `"001"`, `0`, `5`, `1.234567111111111`, `"True"`, SPECIAL_CHAR}
	maptest5Expected := map3Expected{5, SPECIAL_CHAR_EXPECTED_OUTPUT, []byte{0x30, 0x30, 0x31}, 0, 5, 1.234567111111111, true, SPECIAL_CHAR_EXPECTED_OUTPUT}
	transformTest(t, testSourceSchema, transformer, maptest5, maptest5Expected)

	// maptest6 := map3Input{`6`, LARGE_STRING, `""`, `-1`, `0`, `1.7976931348623157e+308`, `"False"`, LARGE_STRING}
	// maptest6Expected := map3Expected{6, LARGE_STRING_EXPECTED_OUTPUT, []byte{}, -1, 0, 1.7976931348623157e+308, false, LARGE_STRING_EXPECTED_OUTPUT}
	// transformTest(t, testSourceSchema, transformer, maptest6, maptest6Expected)

	maptest7 := map3Input{`7`, `""`, `" "`, `5`, `5`, `-1.7976931348623157e+308`, `"T"`, `5`}
	maptest7Expected := map3Expected{7, "", []byte{0x20}, 5, 5, -1.7976931348623157e+308, true, float64(5)}
	transformTest(t, testSourceSchema, transformer, maptest7, maptest7Expected)

	maptest8 := map3Input{`8`, `" "`, `"0"`, `0`, `0`, `-0`, `"F"`, `[]`}
	maptest8Expected := map3Expected{8, " ", []byte{0x30}, 0, 0, 0, false, []interface{}{}}
	transformTest(t, testSourceSchema, transformer, maptest8, maptest8Expected)

	maptest9 := map3Input{`9`, `"abc"`, `"001"`, `-1`, `5`, `5`, `"t"`, `{}`}
	maptest9Expected := map3Expected{9, "abc", []byte{0x30, 0x30, 0x31}, -1, 5, 5, true, map[string]interface{}{}}
	transformTest(t, testSourceSchema, transformer, maptest9, maptest9Expected)

	maptest10 := map3Input{`10`, `"中文"`, `""`, `5`, `0`, `1.23`, `"f"`, `true`}
	maptest10Expected := map3Expected{10, "中文", []byte{}, 5, 0, 1.23, false, true}
	transformTest(t, testSourceSchema, transformer, maptest10, maptest10Expected)

	maptest11 := map3Input{`11`, SPECIAL_CHAR, `""`, `0`, `5`, `-1.23`, `"0"`, `null`}
	maptest11Expected := map3Expected{11, SPECIAL_CHAR_EXPECTED_OUTPUT, []byte{}, 0, 5, -1.23, false, nil}
	transformTest(t, testSourceSchema, transformer, maptest11, maptest11Expected)

	// maptest12 := map3Input{`12`, LARGE_STRING, LARGE_BYTE, `-1`, `0`, `-1.234567111111111`, `"1"`, `""`}
	// maptest12Expected := map3Expected{12, LARGE_STRING_EXPECTED_OUTPUT, LARGE_BYTE_EXPECTED_OUTPUT, -1, 0, -1.234567111111111, true, ""}
	// transformTest(t, testSourceSchema, transformer, maptest12, maptest12Expected)
}

type map3Input struct {
	id         string
	string_col string
	binary_col string
	int_col    string
	uint_col   string
	float_col  string
	bool_col   string
	any_col    string
}

type map3Expected struct {
	id         uint64
	string_col string
	binary_col []byte
	int_col    int64
	uint_col   uint64
	float_col  float64
	bool_col   bool
	any_col    interface{}
}

func transformTest(t *testing.T, testSourceSchema *schemer.Schema, transformer *schemer.Transformer, input map3Input, expected map3Expected) {
	source, err := normalize_map_schema3(testSourceSchema, input)
	if err != nil {
		t.Error(err)
		return
	}
	result, err := transformAndAssert(t, transformer, source)
	if err != nil {
		t.Error(err)
		return
	}
	assertMap3Result(t, result, expected)
}

func normalize_map_schema3(s *schemer.Schema, input map3Input) (map[string]interface{}, error) {
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
	}`, input.id, input.string_col, input.binary_col, input.int_col, input.uint_col, input.float_col, input.bool_col, input.any_col)
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

func assertMap3Result(t *testing.T, result map[string]interface{}, expected map3Expected) {
	assert.Equal(t, expected.id, result["id"])
	map_col := result["map_col"].(map[string]interface{})
	assert.Equal(t, expected.string_col, map_col["string_col"])
	assert.Equal(t, expected.binary_col, map_col["binary_col"])
	assert.Equal(t, expected.int_col, map_col["int_col"])
	assert.Equal(t, expected.uint_col, map_col["uint_col"])
	assert.Equal(t, expected.float_col, map_col["float_col"])
	assert.Equal(t, expected.bool_col, map_col["bool_col"])
	assert.Equal(t, expected.any_col, map_col["any_col"])
}
