package tests

import (
	"testing"

	"github.com/BrobridgeOrg/schemer"
	"github.com/stretchr/testify/assert"
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

func TestNormalSchemer(t *testing.T) {

	testNormalSchema := schemer.NewSchema()
	err := schemer.UnmarshalJSON([]byte(normalSchema), testNormalSchema)
	if err != nil {
		t.Error(err)
	}

	// Using the same schema for destination
	testDestSchema := schemer.NewSchema()
	err = schemer.UnmarshalJSON([]byte(normalSchema), testDestSchema)
	if err != nil {
		t.Error(err)
	}

	// Create transformer
	transformer := schemer.NewTransformer(testNormalSchema, testDestSchema)

	// Set transform script
	transformer.SetScript(`return source`)

	// Preparing source data
	source := testNormalSchema.Normalize(map[string]interface{}{
		"id":         3,
		"string_col": "abc",
		"binary_col": "[0]x32768",
		"int_col":    -1,
		"uint_col":   5,
		"float_col":  -1.23,
		"bool_col":   false,
		"any_col":    "abc",
	})

	// Transforming
	// returnedValue, err := transformer.Transform(nil, source)
	// if assert.Nil(t, err) {
	// 	return
	// }

	// if !assert.Len(t, returnedValue, 1) {
	// 	return
	// }

	// result := returnedValue[0]

	// Normal fields
	assert.Equal(t, uint64(3), source["id"])
	assert.Equal(t, "abc", source["string_col"].(string))
	assert.Equal(t, []byte{0x5b, 0x30, 0x5d, 0x78, 0x33, 0x32, 0x37, 0x36, 0x38}, source["binary_col"])
	assert.Equal(t, int64(-1), source["int_col"])
	assert.Equal(t, uint64(5), source["uint_col"])
	assert.Equal(t, float64(-1.23), source["float_col"])
	assert.Equal(t, false, source["bool_col"])
	assert.Equal(t, "abc", source["any_col"].(string))
}

func TestNotMatchWithExpectResult(t *testing.T) {
	
	testNormalSchema := schemer.NewSchema()
	err := schemer.UnmarshalJSON([]byte(normalSchemaForExtension1), testNormalSchema)
	if err != nil {
		t.Error(err)
	}

	// Using the same schema for destination
	testDestSchema := schemer.NewSchema()
	err = schemer.UnmarshalJSON([]byte(normalSchemaForExtension1), testDestSchema)
	if err != nil {
		t.Error(err)
	}

	// Create transformer
	transformer := schemer.NewTransformer(testNormalSchema, testDestSchema)

	// Set transform script
	transformer.SetScript(`return source`)

	// Preparing source data
	source := testNormalSchema.Normalize(map[string]interface{}{
		"id":         1,
		"string_col": `!@#$%^&*()_+{}:<>?~-=[]\;',./`,
		"binary_col": "abc",
		"int_col":    "",
		"uint_col":   "",
		"float_col":  "",
		"bool_col":   "",
	})

	// Transforming
	// returnedValue, err := transformer.Transform(nil, source)
	// if assert.Nil(t, err) {
	// 	return
	// }

	// if !assert.Len(t, returnedValue, 1) {
	// 	return
	// }

	// result := returnedValue[0]

	// Normal fields
	assert.Equal(t, uint64(1), source["id"])
	assert.Equal(t, `!@#$%^&*()_+{}:<>?~-=[]\;',./`, source["string_col"].(string))
	assert.Equal(t, []byte{0x61,0x62,0x63}, source["binary_col"])
	assert.Equal(t, int64(0), source["int_col"])
	assert.Equal(t, uint64(0x0), source["uint_col"])
	assert.Equal(t, float64(0), source["float_col"])
	assert.Equal(t, false, source["bool_col"])
}