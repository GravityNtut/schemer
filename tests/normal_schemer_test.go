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
	LARGE_BYTE_EXPECTED_OUTPUT = []byte{}
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

	MainSuccessInput2 := Input{`2`, `" "`, `" "`, `0`, `0`, `1.23`, `1`, `" "`}
	MainSuccessExpected2 := Expected{2, " ", []byte{0x20}, 0, 0, 1.23, true, " "}
	transformTest(t, testSourceSchema, transformer, MainSuccessInput2, MainSuccessExpected2)

	MainSuccessInput3 := Input{`3`, `"abc"`, LARGE_BYTE, `-1`, `5`, `-1.23`, `"false"`, `"abc"`}
	MainSuccessExpected3 := Expected{3, "abc", LARGE_BYTE_EXPECTED_OUTPUT, -1, 5, -1.23, false, "abc"}
	transformTest(t, testSourceSchema, transformer, MainSuccessInput3, MainSuccessExpected3)

	MainSuccessInput4 := Input{`4`, `"中文"`, `"0"`, `5`, `0`, `-1.234567111111111`, `"true"`, `"中文"`}
	MainSuccessExpected4 := Expected{4, "中文", []byte{0x30}, 5, 0, -1.234567111111111, true, "中文"}
	transformTest(t, testSourceSchema, transformer, MainSuccessInput4, MainSuccessExpected4)

	MainSuccessInput5 := Input{`5`, SPECIAL_CHAR, `"001"`, `0`, `5`, `1.234567111111111`, `"True"`, SPECIAL_CHAR}
	MainSuccessExpected5 := Expected{5, SPECIAL_CHAR_EXPECTED_OUTPUT, []byte{0x30, 0x30, 0x31}, 0, 5, 1.234567111111111, true, SPECIAL_CHAR_EXPECTED_OUTPUT}
	transformTest(t, testSourceSchema, transformer, MainSuccessInput5, MainSuccessExpected5)

	// MainSuccessInput6 := Input{`6`, `""`, `""`, `-1`, `0`, `-1.7976931348623157e+308`, `"False"`, ""}
	// MainSuccessExpected6 := Expected{6, "", []byte{}, -1, 0, -1.7976931348623157e+308, false, ""}
	// transformTest(t, testSourceSchema, transformer, MainSuccessInput6, MainSuccessExpected6)

	MainSuccessInput7 := Input{`7`, `""`, `" "`, `5`, `5`, `-1.7976931348623157e+308`, `"T"`, `5`}
	MainSuccessExpected7 := Expected{7, "", []byte{0x20}, 5, 5, -1.7976931348623157e+308, true, int64(5)}
	transformTest(t, testSourceSchema, transformer, MainSuccessInput7, MainSuccessExpected7)

	MainSuccessInput8 := Input{`8`, `" "`, `"0"`, `0`, `0`, `-0`, `"F"`, `[]`}
	MainSuccessExpected8 := Expected{8, " ", []byte{0x30}, 0, 0, -0, false, []interface{}{}}
	transformTest(t, testSourceSchema, transformer, MainSuccessInput8, MainSuccessExpected8)

	MainSuccessInput9 := Input{`9`, `"abc"`, `"001"`, `-1`, `5`, `5`, `"t"`, `{}`}
	MainSuccessExpected9 := Expected{9, "abc", []byte{0x30, 0x30, 0x31}, -1, 5, 5, true, map[string]interface{}{}}
	transformTest(t, testSourceSchema, transformer, MainSuccessInput9, MainSuccessExpected9)

	MainSuccessInput10 := Input{`10`, `"中文"`, `""`, `5`, `0`, `1.23`, `"f"`, `true`}
	MainSuccessExpected10 := Expected{10, "中文", []byte{}, 5, 0, 1.23, false, true}
	transformTest(t, testSourceSchema, transformer, MainSuccessInput10, MainSuccessExpected10)

	MainSuccessInput11 := Input{`11`, SPECIAL_CHAR, `" "`, `0`, `5`, `-1.23`, `"0"`, `null`}
	MainSuccessExpected11 := Expected{11, SPECIAL_CHAR_EXPECTED_OUTPUT, []byte{0x20}, 0, 5, -1.23, false, nil}
	transformTest(t, testSourceSchema, transformer, MainSuccessInput11, MainSuccessExpected11)

	// MainSuccessInput12 := Input{`12`, LARGE_STRING, LARGE_BYTE, `-1`, `0`, `-1.234567111111111`, `"1"`, `""`}
	// MainSuccessExpected12 := Expected{12, LARGE_STRING_EXPECTED_OUTPUT, LARGE_BYTE_EXPECTED_OUTPUT, -1, 0, -1.234567111111111, true, ""}
	// transformTest(t, testSourceSchema, transformer, MainSuccessInput12, MainSuccessExpected12)
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
	ExtensionOneExpected1 := ExtensionExpected{uint64(1), "5", []byte{0x61, 0x62, 0x63}, int64(0), uint64(0), float64(0), false}
	transformExtensionTest(t, testSourceSchema, transformer, ExtensionOneInput1, ExtensionOneExpected1)

	ExtensionOneInput2 := ExtensionInput{`2`, `5`, `"中文"`, `" "`, `" "`, `" "`, `" "`}
	ExtensionOneExpected2 := ExtensionExpected{uint64(2), "5", []byte{0xe4, 0xb8, 0xad, 0xe6, 0x96, 0x87}, int64(0), uint64(0), float64(0), false}
	transformExtensionTest(t, testSourceSchema, transformer, ExtensionOneInput2, ExtensionOneExpected2)

	ExtensionOneInput3 := ExtensionInput{`3`, `5`, SPECIAL_CHAR, `"abc"`, `"abc"`, `"abc"`, `"abc"`}
	ExtensionOneExpected3 := ExtensionExpected{uint64(3), "5", []byte(SPECIAL_CHAR_EXPECTED_OUTPUT), int64(0), uint64(0), float64(0), false}
	transformExtensionTest(t, testSourceSchema, transformer, ExtensionOneInput3, ExtensionOneExpected3)

	// ExtensionOneInput4 := ExtensionInput{`4`, `5`, `5`, `"中文"`, `"中文"`, `"中文"`, `"中文"`}
	// ExtensionOneExpected4 := ExtensionExpected{uint64(4), "5", []byte{0x35}, int64(0), uint64(0x0), float64(0), false}
	// transformExtensionTest(t, testSourceSchema, transformer, ExtensionOneInput4, ExtensionOneExpected4)

	ExtensionOneInput5 := ExtensionInput{`5`, `5`, `"10102"`, SPECIAL_CHAR, SPECIAL_CHAR, SPECIAL_CHAR, SPECIAL_CHAR}
	ExtensionOneExpected5 := ExtensionExpected{uint64(5), "5", []byte{0x31, 0x30, 0x31, 0x30, 0x32}, int64(0), uint64(0), float64(0), false}
	transformExtensionTest(t, testSourceSchema, transformer, ExtensionOneInput5, ExtensionOneExpected5)

	// ExtensionOneInput6 := ExtensionInput{`6`, `5`, `101`, LARGE_STRING, LARGE_STRING, LARGE_STRING, LARGE_STRING}
	// ExtensionOneExpected6 := ExtensionExpected{uint64(6), "5", []byte{0x31, 0x30, 0x31}, int64(0), uint64(0x0), float64(0), false}
	// transformExtensionTest(t, testSourceSchema, transformer, ExtensionOneInput6, ExtensionOneExpected6)

	// ExtensionOneInput7 := ExtensionInput{`7`, `5`, `"abc"`, `9223372036854775808`, `-1`, `1.0000000000000001`, `5`}
	// ExtensionOneExpected7 := ExtensionExpected{uint64(7), "5", []byte{0x61, 0x62, 0x63}, int64(0), uint64(0), float64(1), true}
	// transformExtensionTest(t, testSourceSchema, transformer, ExtensionOneInput7, ExtensionOneExpected7)

	// ExtensionOneInput8 := ExtensionInput{`8`, `5`, `"中文"`, `-9223372036854775809`, `18446744073709551616`, `""`, `""`}
	// ExtensionOneExpected8 := ExtensionExpected{uint64(8), "5", []byte{0xe4, 0xb8, 0xad, 0xe6, 0x96, 0x87}, int64(0), uint64(0), float64(0), false}
	// transformExtensionTest(t, testSourceSchema, transformer, ExtensionOneInput8, ExtensionOneExpected8)

	ExtensionOneInput9 := ExtensionInput{`9`, `5`, SPECIAL_CHAR, `1.23`, `1.23`, `" "`, `" "`}
	ExtensionOneExpected9 := ExtensionExpected{uint64(9), "5", []byte(SPECIAL_CHAR_EXPECTED_OUTPUT), int64(1), uint64(1), float64(0), false}
	transformExtensionTest(t, testSourceSchema, transformer, ExtensionOneInput9, ExtensionOneExpected9)
}
