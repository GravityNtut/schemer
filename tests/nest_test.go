package schemer_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/BrobridgeOrg/schemer"
	"github.com/stretchr/testify/assert"
)

func TestArrayArrayString(t *testing.T) {
	stringSchema := `{ "array_col": { "type": "array", "subtype": { "type": "array", "subtype": "string" } } }`
	transformer, sourceSchema := SetupNestTransformer(t, stringSchema)

	var rawData map[string]interface{}

	err := json.Unmarshal([]byte(`{"array_col":[["s11", "s12", "s13"], ["s21", "s22", "s23"], ["s31", "s32", "s33"]]}`), &rawData)
	if err != nil {
		t.Fatal(err)
	}

	source := sourceSchema.Normalize(rawData)
	result := AssertNestTransform(t, transformer, source)

	expected := []interface{}{
		[]interface{}{"s11", "s12", "s13"},
		[]interface{}{"s21", "s22", "s23"},
		[]interface{}{"s31", "s32", "s33"},
	}
	assert.Equal(t, expected, result["array_col"])
}

func TestArrayArrayBinary(t *testing.T) {
	binarySchema := `{ "array_col": { "type": "array", "subtype": { "type": "array", "subtype": "binary" } } }`
	transformer, sourceSchema := SetupNestTransformer(t, binarySchema)

	var rawData map[string]interface{}

	err := json.Unmarshal([]byte(`{"array_col":[["s11", "s12", "s13"], ["s21", "s22", "s23"], ["s31", "s32", "s33"]]}`), &rawData)
	if err != nil {
		t.Fatal(err)
	}

	source := sourceSchema.Normalize(rawData)
	result := AssertNestTransform(t, transformer, source)

	expected := []interface{}{
		[]interface{}{[]byte{0x73, 0x31, 0x31}, []byte{0x73, 0x31, 0x32}, []byte{0x73, 0x31, 0x33}},
		[]interface{}{[]byte{0x73, 0x32, 0x31}, []byte{0x73, 0x32, 0x32}, []byte{0x73, 0x32, 0x33}},
		[]interface{}{[]byte{0x73, 0x33, 0x31}, []byte{0x73, 0x33, 0x32}, []byte{0x73, 0x33, 0x33}},
	}
	assert.Equal(t, expected, result["array_col"])
}

func TestArrayArrayInt(t *testing.T) {
	intSchema := `{ "array_col": { "type": "array", "subtype": { "type": "array", "subtype": "int" } } }`
	transformer, sourceSchema := SetupNestTransformer(t, intSchema)

	var rawData map[string]interface{}

	err := json.Unmarshal([]byte(`{"array_col":[["11", "12", "13"], [21, 22, 23], [31, "32", 33]]}`), &rawData)
	if err != nil {
		t.Fatal(err)
	}

	source := sourceSchema.Normalize(rawData)
	result := AssertNestTransform(t, transformer, source)

	expected := []interface{}{
		[]interface{}{int64(11), int64(12), int64(13)},
		[]interface{}{int64(21), int64(22), int64(23)},
		[]interface{}{int64(31), int64(32), int64(33)},
	}
	assert.Equal(t, expected, result["array_col"])
}

func TestArrayArrayUint(t *testing.T) {
	uintSchema := `{ "array_col": { "type": "array", "subtype": { "type": "array", "subtype": "uint" } } }`
	transformer, sourceSchema := SetupNestTransformer(t, uintSchema)

	var rawData map[string]interface{}

	err := json.Unmarshal([]byte(`{"array_col":[["11", "12", "13"], [21, 22, 23], [31, "32", 33]]}`), &rawData)
	if err != nil {
		t.Fatal(err)
	}

	source := sourceSchema.Normalize(rawData)
	result := AssertNestTransform(t, transformer, source)

	expected := []interface{}{
		[]interface{}{uint64(11), uint64(12), uint64(13)},
		[]interface{}{uint64(21), uint64(22), uint64(23)},
		[]interface{}{uint64(31), uint64(32), uint64(33)},
	}
	assert.Equal(t, expected, result["array_col"])
}

func TestArrayArrayFloat(t *testing.T) {
	floatSchema := `{ "array_col": { "type": "array", "subtype": { "type": "array", "subtype": "float" } } }`
	transformer, sourceSchema := SetupNestTransformer(t, floatSchema)

	var rawData map[string]interface{}

	err := json.Unmarshal([]byte(`{"array_col":[["0.11", "0.12", "0.13"], [10.21, 10.22, 10.23], [100.31, "100.32", 100.33]]}`), &rawData)
	if err != nil {
		t.Fatal(err)
	}

	source := sourceSchema.Normalize(rawData)
	result := AssertNestTransform(t, transformer, source)

	expected := []interface{}{
		[]interface{}{float64(0.11), float64(0.12), float64(0.13)},
		[]interface{}{float64(10.21), float64(10.22), float64(10.23)},
		[]interface{}{float64(100.31), float64(100.32), float64(100.33)},
	}
	assert.Equal(t, expected, result["array_col"])
}

func TestArrayArrayBool(t *testing.T) {
	boolSchema := `{ "array_col": { "type": "array", "subtype": { "type": "array", "subtype": "bool" } } }`
	transformer, sourceSchema := SetupNestTransformer(t, boolSchema)

	var rawData map[string]interface{}

	err := json.Unmarshal([]byte(`{"array_col":[[0, "F", "false"], [1, "T", "true"], [true, "False", "t"]]}`), &rawData)
	if err != nil {
		t.Fatal(err)
	}

	source := sourceSchema.Normalize(rawData)
	result := AssertNestTransform(t, transformer, source)

	expected := []interface{}{
		[]interface{}{false, false, false},
		[]interface{}{true, true, true},
		[]interface{}{true, false, true},
	}
	assert.Equal(t, expected, result["array_col"])
}

func TestArrayArrayTime(t *testing.T) {
	timeSchema := `{ "array_col": { "type": "array", "subtype": { "type": "array", "subtype": "time" } } }`
	transformer, sourceSchema := SetupNestTransformer(t, timeSchema)

	var rawData map[string]interface{}

	err := json.Unmarshal([]byte(`{"array_col":[["2025-02-28T10:00:00Z", "2024-10-11T11:00:00Z", "2023-06-09T12:00:00Z"], ["2013-10-31T00:10:00Z", "2012-09-14T00:11:00Z", "2011-12-17T00:12:00Z"], ["1999-02-13T00:00:10Z", "1998-05-17T00:00:11Z", "1997-03-09T00:00:12Z"]]}`), &rawData)
	if err != nil {
		t.Fatal(err)
	}

	source := sourceSchema.Normalize(rawData)
	result := AssertNestTransform(t, transformer, source)

	expected := []interface{}{
		[]interface{}{time.Date(2025, 2, 28, 10, 0, 0, 0, time.UTC), time.Date(2024, 10, 11, 11, 0, 0, 0, time.UTC), time.Date(2023, 6, 9, 12, 0, 0, 0, time.UTC)},
		[]interface{}{time.Date(2013, 10, 31, 0, 10, 0, 0, time.UTC), time.Date(2012, 9, 14, 0, 11, 0, 0, time.UTC), time.Date(2011, 12, 17, 0, 12, 0, 0, time.UTC)},
		[]interface{}{time.Date(1999, 2, 13, 0, 0, 10, 0, time.UTC), time.Date(1998, 5, 17, 0, 0, 11, 0, time.UTC), time.Date(1997, 3, 9, 0, 0, 12, 0, time.UTC)},
	}
	assert.Equal(t, expected, result["array_col"])
}

func TestArrayArrayAny(t *testing.T) {
	AnySchema := `{ "array_col": { "type": "array", "subtype": { "type": "array", "subtype": "any" } } }`
	transformer, sourceSchema := SetupNestTransformer(t, AnySchema)

	var rawData map[string]interface{}

	err := json.Unmarshal([]byte(`{"array_col":[["2025-02-28T10:00:00Z", "test", "true"], [123, 9.09, true], [0, false, 18446744073709551615]]}`), &rawData)
	if err != nil {
		t.Fatal(err)
	}

	source := sourceSchema.Normalize(rawData)
	result := AssertNestTransform(t, transformer, source)

	expected := []interface{}{
		[]interface{}{"2025-02-28T10:00:00Z", "test", "true"},
		[]interface{}{float64(123), float64(9.09), true},
		[]interface{}{float64(0), false, float64(18446744073709551615)},
	}
	assert.Equal(t, expected, result["array_col"])
}

// 會把非string的變成string
func TestArrayMap(t *testing.T) {
	schema := `{ "array_col": { "type": "array", "subtype": "map", "fields": { 
							"nested_string": { "type": "string" },  
							"nested_binary": { "type": "binary" },
							"nested_int": { "type": "int" },
							"nested_uint": { "type": "uint" },
							"nested_float": { "type": "float" },
							"nested_bool": { "type": "bool" },
							"nested_time": { "type": "time" },
							"nested_any": { "type": "any" }
							} } }`
	transformer, sourceSchema := SetupNestTransformer(t, schema)

	var rawData map[string]interface{}

	err := json.Unmarshal([]byte(
		`{"array_col":[
				{"nested_string": "s1", "nested_binary": "s1", "nested_int": "1", "nested_uint": "1", "nested_float": "1.1", "nested_bool": "true", "nested_time": "2025-02-28T10:00:00Z", "nested_any": "s1"},
				{"nested_string": "s2", "nested_binary": "s2", "nested_int": 2, "nested_uint": 2, "nested_float": 2.2, "nested_bool": true, "nested_time": "2024-10-11T11:00:00Z", "nested_any": 2, "nested_extra": "extra"},
				{"nested_int": "3", "nested_uint": "3", "nested_float": "3.3", "nested_bool": "1", "nested_time": "2023-06-09T12:00:00Z", "nested_any": "s3"}
			]
		}`), &rawData)
	if err != nil {
		t.Fatal(err)
	}

	source := sourceSchema.Normalize(rawData)
	result := AssertNestTransform(t, transformer, source)

	expected := []interface{}{
		map[string]interface{}{"nested_string": "s1", "nested_binary": []byte{0x73, 0x31}, "nested_int": int64(1), "nested_uint": uint64(1), "nested_float": float64(1.1), "nested_bool": true, "nested_time": time.Date(2025, 2, 28, 10, 0, 0, 0, time.UTC), "nested_any": "s1"},
		map[string]interface{}{"nested_string": "s2", "nested_binary": []byte{0x73, 0x32}, "nested_int": int64(2), "nested_uint": uint64(2), "nested_float": float64(2.2), "nested_bool": true, "nested_time": time.Date(2024, 10, 11, 11, 0, 0, 0, time.UTC), "nested_any": float64(2)},
		map[string]interface{}{"nested_int": int64(3), "nested_uint": uint64(3), "nested_float": float64(3.3), "nested_bool": true, "nested_time": time.Date(2023, 6, 9, 12, 0, 0, 0, time.UTC), "nested_any": "s3"},
	}
	assert.Equal(t, expected, result["array_col"])
}

func TestMapArray(t *testing.T) {
	schema := `{ "map_col": { "type": "map", "fields": { 
				"string_array": { "type": "array", "subtype": "string" },
				"binary_array": { "type": "array", "subtype": "binary" },
				"int_array": { "type": "array", "subtype": "int" },
				"uint_array": { "type": "array", "subtype": "uint" },
				"float_array": { "type": "array", "subtype": "float" },
				"bool_array": { "type": "array", "subtype": "bool" },
				"time_array": { "type": "array", "subtype": "time" },
				"any_array": { "type": "array", "subtype": "any" }
				} } }`
	transformer, sourceSchema := SetupNestTransformer(t, schema)

	var rawData map[string]interface{}

	err := json.Unmarshal([]byte(`
		{ "map_col": {
		 "string_array": ["s1", "s2", "s3"],
		 "binary_array": ["s1", "s2", "s3"],
		 "int_array": [1, "2", 3],
		 "uint_array": [1, "2", 3],
		 "float_array": [1.1, "2.2", 3.3],
		 "bool_array": [true, "T", "t"],
		 "time_array": ["2025-02-28T10:00:00Z", "2024-10-11T11:00:00Z", "2023-06-09T12:00:00Z"],
		 "any_array": ["s1", 2, true]
		} }`), &rawData)
	if err != nil {
		t.Fatal(err)
	}

	source := sourceSchema.Normalize(rawData)
	result := AssertNestTransform(t, transformer, source)

	expected := map[string]interface{}{
		"string_array": []interface{}{"s1", "s2", "s3"},
		"binary_array": []interface{}{[]byte{0x73, 0x31}, []byte{0x73, 0x32}, []byte{0x73, 0x33}},
		"int_array":    []interface{}{int64(1), int64(2), int64(3)},
		"uint_array":   []interface{}{uint64(1), uint64(2), uint64(3)},
		"float_array":  []interface{}{float64(1.1), float64(2.2), float64(3.3)},
		"bool_array":   []interface{}{true, true, true},
		"time_array":   []interface{}{time.Date(2025, 2, 28, 10, 0, 0, 0, time.UTC), time.Date(2024, 10, 11, 11, 0, 0, 0, time.UTC), time.Date(2023, 6, 9, 12, 0, 0, 0, time.UTC)},
		"any_array":    []interface{}{"s1", float64(2), true},
	}
	assert.Equal(t, expected, result["map_col"])
}

func TestMapMap(t *testing.T) {
	schema := `{ "map_col": { "type": "map", "fields": { "nested_map": { "type": "map", "fields": { 
				"nested_string": { "type": "string" },
				"nested_binary": { "type": "binary" },
				"nested_int": { "type": "int" },
				"nested_uint": { "type": "uint" },
				"nested_float": { "type": "float" },
				"nested_bool": { "type": "bool" },
				"nested_time": { "type": "time" },
				"nested_any": { "type": "any" }
				} } } } }`

	transformer, sourceSchema := SetupNestTransformer(t, schema)

	var rawData map[string]interface{}
	err := json.Unmarshal([]byte(`
	{ "map_col": { "nested_map": {
		"nested_string": "s1",
		"nested_binary": "s1",
		"nested_int": "-199631",
		"nested_uint": "901213",
		"nested_float": "548.253",
		"nested_bool": "true",
		"nested_time": "2025-01-30T10:00:00Z",
		"nested_any": false
	} } }`), &rawData)
	if err != nil {
		t.Fatal(err)
	}
	source := sourceSchema.Normalize(rawData)
	result := AssertNestTransform(t, transformer, source)

	expected := map[string]interface{}{"nested_map": map[string]interface{}{
		"nested_string": "s1",
		"nested_binary": []byte{0x73, 0x31},
		"nested_int":    int64(-199631),
		"nested_uint":   uint64(901213),
		"nested_float":  float64(548.253),
		"nested_bool":   true,
		"nested_time":   time.Date(2025, 1, 30, 10, 0, 0, 0, time.UTC),
		"nested_any":    false,
	}}
	assert.Equal(t, expected, result["map_col"])
}

func TestLongNested(t *testing.T) {
	schema := `{"array_col": {
				"type": "array",
				"subtype": "map",
				"fields": {
				"nested1_array": {
					"type": "array",
					"subtype": "map",
					"fields": {
					"nested_array2": {
						"type": "array",
						"subtype": "map",
						"fields": {
						"nested_array3": {
							"type": "array",
							"subtype": "map",
							"fields": {
							"nested_array4": {
								"type": "array",
								"subtype": "float"
							}}}}}}}}}}`

	transformer, sourceSchema := SetupNestTransformer(t, schema)
	var rawData map[string]interface{}

	data := `{"array_col": [
		    {"nested_array1": [
				{"nested_array2": [
						{"nested_array3": [
								{"nested_array4": [4.11, 4.12, 4.13]},{"nested_array4": [4.21, "4.22", 4.23]}
						]},
						{"nested_array3": [
								{"nested_array4": ["4.31", 4.32, 4.33]}
						]}
				]},
				{"nested_array2": [
						{"nested_array3": [
								{"nested_array4": [4.41, 4.42, "4.43"]}
						]}
				]}
			]},
			{"nested_array1": []}
		  ]}`

	err := json.Unmarshal([]byte(data), &rawData)
	if err != nil {
		t.Fatal(err)
	}

	source := sourceSchema.Normalize(rawData)
	result := AssertNestTransform(t, transformer, source)

	expected := []interface{}{
		map[string]interface{}{
			"nested_array1": []interface{}{
				map[string]interface{}{
					"nested_array2": []interface{}{
						map[string]interface{}{
							"nested_array3": []interface{}{
								map[string]interface{}{
									"nested_array4": []interface{}{float64(4.11), float64(4.12), float64(4.13)},
								},
								map[string]interface{}{
									"nested_array4": []interface{}{float64(4.21), float64(4.22), float64(4.23)},
								},
							},
						},
						map[string]interface{}{
							"nested_array3": []interface{}{
								map[string]interface{}{
									"nested_array4": []interface{}{float64(4.31), float64(4.32), float64(4.33)},
								},
							},
						},
					},
				},
				map[string]interface{}{
					"nested_array2": []interface{}{
						map[string]interface{}{
							"nested_array3": []interface{}{
								map[string]interface{}{"nested_array4": []interface{}{float64(4.41), float64(4.42), float64(4.43)}},
							},
						},
					},
				},
			},
		},
		map[string]interface{}{"nested_array1": []interface{}{}},
	}
	assert.Equal(t, expected, result["array_col"])
}

func AssertNestTransform(t *testing.T, transformer *schemer.Transformer, source map[string]interface{}) map[string]interface{} {

	returnedValue, err := transformer.Transform(nil, source)
	if !assert.Nil(t, err) {
		t.Fatal(err)
	}

	if !assert.Len(t, returnedValue, 1) {
		t.Fatal(err)
	}

	result := returnedValue[0]
	return result
}

func SetupNestTransformer(t *testing.T, schema string) (*schemer.Transformer, *schemer.Schema) {

	testSourceSchema := schemer.NewSchema()
	err := schemer.UnmarshalJSON([]byte(schema), testSourceSchema)
	if err != nil {
		t.Error(err)
	}

	// Using the same schema for destination
	testDestSchema := schemer.NewSchema()
	err = schemer.UnmarshalJSON([]byte(schema), testDestSchema)
	if err != nil {
		t.Error(err)
	}

	// Create transformer
	transformer := schemer.NewTransformer(testSourceSchema, testDestSchema, schemer.WithRuntime(jsRuntime))
	err = transformer.SetScript(`return source`)
	if err != nil {
		t.Error(err)
	}

	return transformer, testSourceSchema
}
