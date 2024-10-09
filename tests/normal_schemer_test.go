package tests

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/BrobridgeOrg/schemer"
	"github.com/stretchr/testify/assert"
)

type Input struct {
	id         string
	string_col string
	binary_col string
	int_col    string
	uint_col   string
	float_col  string
	bool_col   string
	any_col    string
}

type Expected struct {
	id         uint64
	string_col string
	binary_col []byte
	int_col    int64
	uint_col   uint64
	float_col  float64
	bool_col   bool
	any_col    interface{}
}

type ExtensionInput struct {
	id         string
	string_col string
	binary_col string
	int_col    string
	uint_col   string
	float_col  string
	bool_col   string
}

type ExtensionExpected struct {
	id         uint64
	string_col string
	binary_col []byte
	int_col    int64
	uint_col   uint64
	float_col  float64
	bool_col   bool
}

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

var normalSchema = `{
	"id":{
	   "type":"uint"
	},
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
}`

var normalSchemaForExtension1 = `{
	"id":{
	   "type":"uint"
	},
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
	}
}`

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

func transformTest(t *testing.T, testSourceSchema *schemer.Schema, transformer *schemer.Transformer, input Input, expected Expected) {
	source, err := normalize_normal_schema(testSourceSchema, input)
	if err != nil {
		t.Error(err)
		return
	}
	result, err := transformAndAssert(t, transformer, source)
	if err != nil {
		t.Error(err)
		return
	}
	assertResult(t, result, expected)
}

func transformExtensionTest(t *testing.T, testSourceSchema *schemer.Schema, transformer *schemer.Transformer, input ExtensionInput, expected ExtensionExpected) {
	source, err := normalize_Extension(testSourceSchema, input)
	if err != nil {
		t.Error(err)
		return
	}
	result, err := transformAndAssert(t, transformer, source)
	if err != nil {
		t.Error(err)
		return
	}
	assertExtensionResult(t, result, expected)
}

func normalize_Extension(s *schemer.Schema, input ExtensionInput) (map[string]interface{}, error) {
	jsonInput := fmt.Sprintf(`
	{
		"id":         %s,
		"string_col": %s,
		"binary_col": %s,
		"int_col":    %s,
		"uint_col":   %s,
		"float_col":  %s,
		"bool_col":   %s
	}`, input.id, input.string_col, input.binary_col, input.int_col, input.uint_col, input.float_col, input.bool_col)
	var rawData map[string]interface{}
	err := json.Unmarshal([]byte(jsonInput), &rawData)
	if err != nil {
		return nil, err
	}
	return s.Normalize(rawData), nil
}

func normalize_normal_schema(s *schemer.Schema, input Input) (map[string]interface{}, error) {
	jsonInput := fmt.Sprintf(`
	{
		"id":         %s,
		"string_col": %s,
		"binary_col": %s,
		"int_col":    %s,
		"uint_col":   %s,
		"float_col":  %s,
		"bool_col":   %s,
		"any_col":    %s
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

func assertResult(t *testing.T, result map[string]interface{}, expected Expected) {
	assert.Equal(t, expected.id, result["id"])
	assert.Equal(t, expected.string_col, result["string_col"])
	assert.Equal(t, expected.binary_col, result["binary_col"])
	assert.Equal(t, expected.int_col, result["int_col"])
	assert.Equal(t, expected.uint_col, result["uint_col"])
	assert.Equal(t, expected.float_col, result["float_col"])
	assert.Equal(t, expected.bool_col, result["bool_col"])
	assert.Equal(t, expected.any_col, result["any_col"])
}

func assertExtensionResult(t *testing.T, result map[string]interface{}, expected ExtensionExpected) {
	assert.Equal(t, expected.id, result["id"])
	assert.Equal(t, expected.string_col, result["string_col"])
	assert.Equal(t, expected.binary_col, result["binary_col"])
	assert.Equal(t, expected.int_col, result["int_col"])
	assert.Equal(t, expected.uint_col, result["uint_col"])
	assert.Equal(t, expected.float_col, result["float_col"])
	assert.Equal(t, expected.bool_col, result["bool_col"])
}

func TestNormalSchemer(t *testing.T) {
	testSourceSchema := schemer.NewSchema()
	err := schemer.UnmarshalJSON([]byte(normalSchema), testSourceSchema)
	if err != nil {
		t.Error(err)
		return
	}
	testDestSchema := schemer.NewSchema()
	err = schemer.UnmarshalJSON([]byte(normalSchema), testDestSchema)
	if err != nil {
		t.Error(err)
		return
	}
	transformer := schemer.NewTransformer(testSourceSchema, testDestSchema)
	transformer.SetScript(`return source`)

	MainSuccessInput1 := Input{`1`, `""`, `""`, `5`, `5`, `5`, `0`, `""`}
	MainSuccessExpected1 := Expected{1, "", []byte{}, 5, 5, 5, false, ""}
	transformTest(t, testSourceSchema, transformer, MainSuccessInput1, MainSuccessExpected1)
}

func TestNotMatchWithExpectResult(t *testing.T) {
	testSourceSchema := schemer.NewSchema()
	err := schemer.UnmarshalJSON([]byte(normalSchemaForExtension1), testSourceSchema)
	if err != nil {
		t.Error(err)
		return
	}
	testDestSchema := schemer.NewSchema()
	err = schemer.UnmarshalJSON([]byte(normalSchemaForExtension1), testDestSchema)
	if err != nil {
		t.Error(err)
		return
	}
	transformer := schemer.NewTransformer(testSourceSchema, testDestSchema)
	transformer.SetScript(`return source`)

	ExtensionOneInput1 := ExtensionInput{`1`, `5`, `"abc"`, `""`, `""`, `""`, `""`}
	ExtensionOneExpected1 := ExtensionExpected{uint64(1), "5", []byte{0x61, 0x62, 0x63}, int64(0), uint64(0x0), float64(0), false}
	transformExtensionTest(t, testSourceSchema, transformer, ExtensionOneInput1, ExtensionOneExpected1)
}
