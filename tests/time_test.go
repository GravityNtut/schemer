package tests

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/BrobridgeOrg/schemer"
	"github.com/stretchr/testify/assert"
)

var testTimeSuccessSource = `{
	"time_default": {
	    "type": "time"
	},
	"time_second": {
		"type": "time",
		"precision": "second"
	},
	"time_millisecond": {
		"type": "time",
		"precision": "millisecond"
	},
	"time_microsecond": {
		"type": "time",
		"precision": "microsecond"
	},
  	"time_nanosecond": {
		"type": "time",
		"precision": "nanosecond"
	}
}`

var testTimeSource = `{
	"time_default": {
	    "type": "time"
	},
	"time_second": {
		"type": "time",
		"precision": "second"
	},
	"time_millisecond": {
		"type": "time",
		"precision": "millisecond"
	},
	"time_microsecond": {
		"type": "time",
		"precision": "microsecond"
	},
	"time_notSupport": {
		"type": "time",
		"precision": "notSupport"
	},
	"time_us": {
		"type": "time",
		"precision": "us"
	},
  	"time_nanosecond": {
		"type": "time",
		"precision": "nanosecond"
	},
	"time_null": {
		"type": "time",
		"precision": ""
	},
	"time_us": {
		"type": "time",
		"precision": "us"
	},
	"time_MICROSecond": {
		"type": "time",
		"precision": "MICROSecond"
	}
}`

type timeInput struct {
	time_default     string
	time_second      string
	time_millisecond string
	time_microsecond string
	time_nanosecond  string
}
type timeExpected struct {
	time_default     time.Time
	time_second      time.Time
	time_millisecond time.Time
	time_microsecond time.Time
	time_nanosecond  time.Time
}

func normalize_time_schema(s *schemer.Schema, input timeInput) (map[string]interface{}, error) {
	jsonInput := fmt.Sprintf(`
	{
		"time_default": "%s",
		"time_second": "%s",
		"time_millisecond": "%s",
		"time_microsecond": "%s",
		"time_nanosecond": "%s"
	}`, input.time_default, input.time_second, input.time_millisecond, input.time_microsecond, input.time_nanosecond)
	var rawData map[string]interface{}
	err := json.Unmarshal([]byte(jsonInput), &rawData)
	if err != nil {
		return nil, err
	}
	return s.Normalize(rawData), nil
}

func transformAndAssert(t *testing.T, transformer *schemer.Transformer, source map[string]interface{}) (map[string]interface{}, error) {
	result, err := transformer.Transform(nil, source)
	if err != nil {
		return nil, err
	}
	return result[0], nil
}

func assertTimeResult(t *testing.T, result map[string]interface{}, expected timeExpected) {
	if result["time_default"] != nil {
		assert.Equal(t, expected.time_default, result["time_default"].(time.Time))
	}
	if result["time_second"] != nil {
		assert.Equal(t, expected.time_second, result["time_second"].(time.Time))
	}
	if result["time_millisecond"] != nil {
		assert.Equal(t, expected.time_millisecond, result["time_millisecond"].(time.Time))
	}
	if result["time_microsecond"] != nil {
		assert.Equal(t, expected.time_microsecond, result["time_microsecond"].(time.Time))
	}
	if result["time_nanosecond"] != nil {
		assert.Equal(t, expected.time_nanosecond, result["time_nanosecond"].(time.Time))
	}
}

func transformTest(t *testing.T, testSourceSchema *schemer.Schema, transformer *schemer.Transformer, input timeInput, expected timeExpected) {
	source, err := normalize_time_schema(testSourceSchema, input)
	if err != nil {

		t.Error(err)
		return
	}
	result, err := transformAndAssert(t, transformer, source)
	if err != nil {
		t.Error(err)
		return
	}
	fmt.Println("Result: ", result)
	assertTimeResult(t, result, expected)
}

func TestTimeSuccessTransformer(t *testing.T) {
	testTimeSourceSchema := schemer.NewSchema()
	err := schemer.UnmarshalJSON([]byte(testTimeSuccessSource), testTimeSourceSchema)
	if err != nil {
		t.Error(err)
	}

	destSchema := schemer.NewSchema()
	err = schemer.UnmarshalJSON([]byte(testTimeSuccessSource), destSchema)
	if err != nil {
		t.Error(err)
	}
	// Create transformer
	transformer := schemer.NewTransformer(testTimeSourceSchema, destSchema)

	// Set transform script
	transformer.SetScript(`return source`)

	// second
	// 2024-08-06T15:02:00Z
	timetest1 := timeInput{"", "2024-08-06T15:02:00Z", "", "", ""}
	timetest1Expected := timeExpected{time.Time{}, time.Date(2024, 8, 6, 23, 2, 0, 0, time.Local), time.Time{}, time.Time{}, time.Time{}}
	transformTest(t, testTimeSourceSchema, transformer, timetest1, timetest1Expected)
	// 2024-08-06 15:02:00 (MySQL, Sql Server format)
	timetest2 := timeInput{"", "2024-08-06 15:02:00", "", "", ""}
	timetest2Expected := timeExpected{time.Time{}, time.Date(2024, 8, 6, 23, 2, 0, 0, time.Local), time.Time{}, time.Time{}, time.Time{}}
	transformTest(t, testTimeSourceSchema, transformer, timetest2, timetest2Expected)
	// 2024-08-06T15:02:00+08:00
	timetest3 := timeInput{"", "2024-08-06T15:02:00+08:00", "", "", ""}
	timetest3Expected := timeExpected{time.Time{}, time.Date(2024, 8, 6, 15, 2, 0, 0, time.Local), time.Time{}, time.Time{}, time.Time{}}
	transformTest(t, testTimeSourceSchema, transformer, timetest3, timetest3Expected)

	// millisecond
	//2024-08-06T15:02:00Z
	timetest4 := timeInput{"", "", "2024-08-06T15:02:00", "", ""}
	timetest4Expected := timeExpected{time.Time{}, time.Time{}, time.Date(2024, 8, 6, 23, 2, 0, 0, time.Local), time.Time{}, time.Time{}}
	transformTest(t, testTimeSourceSchema, transformer, timetest4, timetest4Expected)
	//2024-08-06T15:02:00.123Z
	timetest5 := timeInput{"", "", "2024-08-06T15:02:00.123Z", "", ""}
	timetest5Expected := timeExpected{time.Time{}, time.Time{}, time.Date(2024, 8, 6, 23, 2, 0, 123000000, time.Local), time.Time{}, time.Time{}}
	transformTest(t, testTimeSourceSchema, transformer, timetest5, timetest5Expected)
	//2024-08-06 15:02:00 (MySQL, Sql Server format)
	timetest6 := timeInput{"", "", "2024-08-06 15:02:00", "", ""}
	timetest6Expected := timeExpected{time.Time{}, time.Time{}, time.Date(2024, 8, 6, 23, 2, 0, 0, time.Local), time.Time{}, time.Time{}}
	transformTest(t, testTimeSourceSchema, transformer, timetest6, timetest6Expected)
	//2024-08-06T15:02:00+08:00
	timetest7 := timeInput{"", "", "2024-08-06T15:02:00+08:00", "", ""}
	timetest7Expected := timeExpected{time.Time{}, time.Time{}, time.Date(2024, 8, 6, 15, 2, 0, 0, time.Local), time.Time{}, time.Time{}}
	transformTest(t, testTimeSourceSchema, transformer, timetest7, timetest7Expected)

	// microsecond
	// 2024-08-06T15:02:00Z
	timetest8 := timeInput{"", "", "", "2024-08-06T15:02:00Z", ""}
	timetest8Expected := timeExpected{time.Time{}, time.Time{}, time.Time{}, time.Date(2024, 8, 6, 23, 2, 0, 0, time.Local).UTC(), time.Time{}}
	transformTest(t, testTimeSourceSchema, transformer, timetest8, timetest8Expected)
	// 2024-08-06T15:02:00.123Z
	timetest9 := timeInput{"", "", "", "2024-08-06T15:02:00.123Z", ""}
	timetest9Expected := timeExpected{time.Time{}, time.Time{}, time.Time{}, time.Date(2024, 8, 6, 23, 2, 0, 123000000, time.Local).UTC(), time.Time{}}
	transformTest(t, testTimeSourceSchema, transformer, timetest9, timetest9Expected)
	// 2024-08-06T15:02:00.123456Z
	timetest10 := timeInput{"", "", "", "2024-08-06T15:02:00.123456Z", ""}
	timetest10Expected := timeExpected{time.Time{}, time.Time{}, time.Time{}, time.Date(2024, 8, 6, 23, 2, 0, 123456000, time.Local).UTC(), time.Time{}}
	transformTest(t, testTimeSourceSchema, transformer, timetest10, timetest10Expected)
	// 2024-08-06 15:02:00 (Mysql, Sql Server format)
	timetest11 := timeInput{"", "", "", "2024-08-06 15:02:00", ""}
	timetest11Expected := timeExpected{time.Time{}, time.Time{}, time.Time{}, time.Date(2024, 8, 6, 23, 2, 0, 0, time.Local).UTC(), time.Time{}}
	transformTest(t, testTimeSourceSchema, transformer, timetest11, timetest11Expected)
	// 2024-08-06T15:02:00+08:00
	timetest12 := timeInput{"", "", "", "2024-08-06T15:02:00+08:00", ""}
	timetest12Expected := timeExpected{time.Time{}, time.Time{}, time.Time{}, time.Date(2024, 8, 6, 15, 2, 0, 0, time.Local), time.Time{}}
	transformTest(t, testTimeSourceSchema, transformer, timetest12, timetest12Expected)

	// nanosecond
	// 2024-08-06T15:02:00Z
	timetest13 := timeInput{"", "", "", "", "2024-08-06T15:02:00Z"}
	timetest13Expected := timeExpected{time.Time{}, time.Time{}, time.Time{}, time.Time{}, time.Date(2024, 8, 6, 23, 2, 0, 0, time.Local)}
	transformTest(t, testTimeSourceSchema, transformer, timetest13, timetest13Expected)
	// 2024-08-06T15:02:00.123Z
	timetest14 := timeInput{"", "", "", "", "2024-08-06T15:02:00.123Z"}
	timetest14Expected := timeExpected{time.Time{}, time.Time{}, time.Time{}, time.Time{}, time.Date(2024, 8, 6, 23, 2, 0, 123000000, time.Local)}
	transformTest(t, testTimeSourceSchema, transformer, timetest14, timetest14Expected)
	// 2024-08-06T15:02:00.123456Z
	// timetest15 := timeInput{"", "", "", "", "2024-08-06T15:02:00.123456Z"}
	// // timetest15 := timeInput{"", "", "", "", "1722956520123456000"}
	// timetest15Expected := timeExpected{time.Time{}, time.Time{}, time.Time{}, time.Time{}, time.Unix(1722956520, 123456000)}
	// transformTest(t, testTimeSourceSchema, transformer, timetest15, timetest15Expected)
	// // 2024-08-06T15:02:00.123456789Z
	// timetest16 := timeInput{"", "", "", "", "2024-08-06T15:02:00.123456789Z"}
	// timetest16Expected := timeExpected{time.Time{}, time.Time{}, time.Time{}, time.Time{}, time.Unix(1722956520, 123456789)}
	// transformTest(t, testTimeSourceSchema, transformer, timetest16, timetest16Expected)
	// 2024-08-06 15:02:00 (Mysql, Sql Server format)
	timetest17 := timeInput{"", "", "", "", "2024-08-06 15:02:00"}
	timetest17Expected := timeExpected{time.Time{}, time.Time{}, time.Time{}, time.Time{}, time.Date(2024, 8, 6, 23, 2, 0, 0, time.Local)}
	transformTest(t, testTimeSourceSchema, transformer, timetest17, timetest17Expected)
	// 2024-08-06T15:02:00+08:00
	timetest18 := timeInput{"", "", "", "", "2024-08-06T15:02:00+08:00"}
	timetest18Expected := timeExpected{time.Time{}, time.Time{}, time.Time{}, time.Time{}, time.Date(2024, 8, 6, 15, 2, 0, 0, time.Local)}
	transformTest(t, testTimeSourceSchema, transformer, timetest18, timetest18Expected)

	// default
	// 2024-08-06T15:02:00Z
	timetest19 := timeInput{"2024-08-06T15:02:00Z", "", "", "", ""}
	timetest19Expected := timeExpected{time.Date(2024, 8, 6, 23, 2, 0, 0, time.Local), time.Time{}, time.Time{}, time.Time{}, time.Time{}}
	transformTest(t, testTimeSourceSchema, transformer, timetest19, timetest19Expected)
	// 2024-08-06T15:02:00.123Z
	timetest20 := timeInput{"2024-08-06T15:02:00.123Z", "", "", "", ""}
	timetest20Expected := timeExpected{time.Date(2024, 8, 6, 23, 2, 0, 123000000, time.Local), time.Time{}, time.Time{}, time.Time{}, time.Time{}}
	transformTest(t, testTimeSourceSchema, transformer, timetest20, timetest20Expected)
	// 2024-08-06 15:02:00 (Mysql, Sql Server format)
	timetest21 := timeInput{"2024-08-06 15:02:00", "", "", "", ""}
	timetest21Expected := timeExpected{time.Date(2024, 8, 6, 23, 2, 0, 0, time.Local), time.Time{}, time.Time{}, time.Time{}, time.Time{}}
	transformTest(t, testTimeSourceSchema, transformer, timetest21, timetest21Expected)
	// 2024-08-06T15:02:00+08:00
	timetest22 := timeInput{"2024-08-06T15:02:00+08:00", "", "", "", ""}
	timetest22Expected := timeExpected{time.Date(2024, 8, 6, 15, 2, 0, 0, time.Local), time.Time{}, time.Time{}, time.Time{}, time.Time{}}
	transformTest(t, testTimeSourceSchema, transformer, timetest22, timetest22Expected)
}

func TestTimeFailurePrecisionTransformer(t *testing.T) {
	testTimeSourceSchema := schemer.NewSchema()
	err := schemer.UnmarshalJSON([]byte(testTimeSuccessSource), testTimeSourceSchema)
	if err != nil {
		t.Error(err)
	}

	destSchema := schemer.NewSchema()
	err = schemer.UnmarshalJSON([]byte(testTimeSuccessSource), destSchema)
	if err != nil {
		t.Error(err)
	}
	// Create transformer
	transformer := schemer.NewTransformer(testTimeSourceSchema, destSchema)

	// Set transform script
	transformer.SetScript(`return source`)

	// precision test
	// timetest1 := timeInput{"", "2024-08-06T15:02:00.1223456789Z", "", "", ""}
	// timetest1Expected := timeExpected{time.Time{}, time.Date(2024, 8, 6, 23, 2, 0, 0, time.Local), time.Time{}, time.Time{}, time.Time{}}
	// transformTest(t, testTimeSourceSchema, transformer, timetest1, timetest1Expected)
	timetest2 := timeInput{"", "", "2024-08-06 15:02:00.123456789Z", "", ""}
	timetest2Expected := timeExpected{time.Time{}, time.Time{}, time.Date(2024, 8, 6, 23, 2, 0, 123000000, time.Local), time.Time{}, time.Time{}}
	transformTest(t, testTimeSourceSchema, transformer, timetest2, timetest2Expected)
	// timetest3 := timeInput{"", "", "", "2024-08-06T15:02:00.123456789Z", ""}
	// timetest3Expected := timeExpected{time.Time{}, time.Time{}, time.Time{}, time.Date(2024, 8, 6, 15, 2, 0, 123456000, time.Local), time.Time{}}
	// transformTest(t, testTimeSourceSchema, transformer, timetest3, timetest3Expected)
	// timetest4 := timeInput{"", "", "", "", "2024-08-06T15:02:00.1234567890Z"}
	// timetest4Expected := timeExpected{time.Time{}, time.Time{}, time.Time{}, time.Time{}, time.Date(2024, 8, 6, 23, 2, 0, 123456789, time.Local)}
	// transformTest(t, testTimeSourceSchema, transformer, timetest4, timetest4Expected)
	timetest5 := timeInput{"2024-08-06T15:02:00.123456789Z", "", "", "", ""}
	timetest5Expected := timeExpected{time.Date(2024, 8, 6, 23, 2, 0, 123000000, time.Local), time.Time{}, time.Time{}, time.Time{}, time.Time{}}
	transformTest(t, testTimeSourceSchema, transformer, timetest5, timetest5Expected)

	// second
	// 2024-08-06T15:02:00Z
	// timetest1 := timeInput{"", "2024-08-06T15:02:00.1223456789Z", "", "", ""}
	// timetest1Expected := timeExpected{time.Time{}, time.Date(2024, 8, 6, 23, 2, 0, 000000000, time.Local), time.Time{}, time.Time{}, time.Time{}}
	// transformTest(t, testTimeSourceSchema, transformer, timetest1, timetest1Expected)
	// // 2024-08-06 15:02:00 (MySQL, Sql Server format)
	// timetest2 := timeInput{"", "2024-08-06 15:02:00", "", "", ""}
	// timetest2Expected := timeExpected{time.Time{}, time.Date(2024, 8, 6, 23, 2, 0, 0, time.Local), time.Time{}, time.Time{}, time.Time{}}
	// transformTest(t, testTimeSourceSchema, transformer, timetest2, timetest2Expected)
	// // 2024-08-06T15:02:00+08:00
	// timetest3 := timeInput{"", "2024-08-06T15:02:00+08:00", "", "", ""}
	// timetest3Expected := timeExpected{time.Time{}, time.Date(2024, 8, 6, 15, 2, 0, 0, time.Local), time.Time{}, time.Time{}, time.Time{}}
	// transformTest(t, testTimeSourceSchema, transformer, timetest3, timetest3Expected)

	// // millisecond
	// //2024-08-06T15:02:00Z
	// timetest4 := timeInput{"", "", "2024-08-06T15:02:00", "", ""}
	// timetest4Expected := timeExpected{time.Time{}, time.Time{}, time.Date(2024, 8, 6, 23, 2, 0, 0, time.Local), time.Time{}, time.Time{}}
	// transformTest(t, testTimeSourceSchema, transformer, timetest4, timetest4Expected)
	// //2024-08-06T15:02:00.123Z
	// timetest5 := timeInput{"", "", "2024-08-06T15:02:00.123Z", "", ""}
	// timetest5Expected := timeExpected{time.Time{}, time.Time{}, time.Date(2024, 8, 6, 23, 2, 0, 123000000, time.Local), time.Time{}, time.Time{}}
	// transformTest(t, testTimeSourceSchema, transformer, timetest5, timetest5Expected)
	// //2024-08-06 15:02:00 (MySQL, Sql Server format)
	// timetest6 := timeInput{"", "", "2024-08-06 15:02:00", "", ""}
	// timetest6Expected := timeExpected{time.Time{}, time.Time{}, time.Date(2024, 8, 6, 23, 2, 0, 0, time.Local), time.Time{}, time.Time{}}
	// transformTest(t, testTimeSourceSchema, transformer, timetest6, timetest6Expected)
	// //2024-08-06T15:02:00+08:00
	// timetest7 := timeInput{"", "", "2024-08-06T15:02:00+08:00", "", ""}
	// timetest7Expected := timeExpected{time.Time{}, time.Time{}, time.Date(2024, 8, 6, 15, 2, 0, 0, time.Local), time.Time{}, time.Time{}}
	// transformTest(t, testTimeSourceSchema, transformer, timetest7, timetest7Expected)

	// // microsecond
	// // 2024-08-06T15:02:00Z
	// timetest8 := timeInput{"", "", "", "2024-08-06T15:02:00Z", ""}
	// timetest8Expected := timeExpected{time.Time{}, time.Time{}, time.Time{}, time.Date(2024, 8, 6, 23, 2, 0, 0, time.Local).UTC(), time.Time{}}
	// transformTest(t, testTimeSourceSchema, transformer, timetest8, timetest8Expected)
	// // 2024-08-06T15:02:00.123Z
	// timetest9 := timeInput{"", "", "", "2024-08-06T15:02:00.123Z", ""}
	// timetest9Expected := timeExpected{time.Time{}, time.Time{}, time.Time{}, time.Date(2024, 8, 6, 23, 2, 0, 123000000, time.Local).UTC(), time.Time{}}
	// transformTest(t, testTimeSourceSchema, transformer, timetest9, timetest9Expected)
	// // 2024-08-06T15:02:00.123456Z
	// timetest10 := timeInput{"", "", "", "2024-08-06T15:02:00.123456Z", ""}
	// timetest10Expected := timeExpected{time.Time{}, time.Time{}, time.Time{}, time.Date(2024, 8, 6, 23, 2, 0, 123456000, time.Local).UTC(), time.Time{}}
	// transformTest(t, testTimeSourceSchema, transformer, timetest10, timetest10Expected)
	// // 2024-08-06 15:02:00 (Mysql, Sql Server format)
	// timetest11 := timeInput{"", "", "", "2024-08-06 15:02:00", ""}
	// timetest11Expected := timeExpected{time.Time{}, time.Time{}, time.Time{}, time.Date(2024, 8, 6, 23, 2, 0, 0, time.Local).UTC(), time.Time{}}
	// transformTest(t, testTimeSourceSchema, transformer, timetest11, timetest11Expected)
	// // 2024-08-06T15:02:00+08:00
	// timetest12 := timeInput{"", "", "", "2024-08-06T15:02:00+08:00", ""}
	// timetest12Expected := timeExpected{time.Time{}, time.Time{}, time.Time{}, time.Date(2024, 8, 6, 15, 2, 0, 0, time.Local), time.Time{}}
	// transformTest(t, testTimeSourceSchema, transformer, timetest12, timetest12Expected)

	// // nanosecond
	// // 2024-08-06T15:02:00Z
	// timetest13 := timeInput{"", "", "", "", "2024-08-06T15:02:00Z"}
	// timetest13Expected := timeExpected{time.Time{}, time.Time{}, time.Time{}, time.Time{}, time.Date(2024, 8, 6, 23, 2, 0, 0, time.Local)}
	// transformTest(t, testTimeSourceSchema, transformer, timetest13, timetest13Expected)
	// // 2024-08-06T15:02:00.123Z
	// timetest14 := timeInput{"", "", "", "", "2024-08-06T15:02:00.123Z"}
	// timetest14Expected := timeExpected{time.Time{}, time.Time{}, time.Time{}, time.Time{}, time.Date(2024, 8, 6, 23, 2, 0, 123000000, time.Local)}
	// transformTest(t, testTimeSourceSchema, transformer, timetest14, timetest14Expected)
	// // 2024-08-06T15:02:00.123456Z
	// // timetest15 := timeInput{"", "", "", "", "2024-08-06T15:02:00.123456Z"}
	// // // timetest15 := timeInput{"", "", "", "", "1722956520123456000"}
	// // timetest15Expected := timeExpected{time.Time{}, time.Time{}, time.Time{}, time.Time{}, time.Unix(1722956520, 123456000)}
	// // transformTest(t, testTimeSourceSchema, transformer, timetest15, timetest15Expected)
	// // // 2024-08-06T15:02:00.123456789Z
	// // timetest16 := timeInput{"", "", "", "", "2024-08-06T15:02:00.123456789Z"}
	// // timetest16Expected := timeExpected{time.Time{}, time.Time{}, time.Time{}, time.Time{}, time.Unix(1722956520, 123456789)}
	// // transformTest(t, testTimeSourceSchema, transformer, timetest16, timetest16Expected)
	// // 2024-08-06 15:02:00 (Mysql, Sql Server format)
	// timetest17 := timeInput{"", "", "", "", "2024-08-06 15:02:00"}
	// timetest17Expected := timeExpected{time.Time{}, time.Time{}, time.Time{}, time.Time{}, time.Date(2024, 8, 6, 23, 2, 0, 0, time.Local)}
	// transformTest(t, testTimeSourceSchema, transformer, timetest17, timetest17Expected)
	// // 2024-08-06T15:02:00+08:00
	// timetest18 := timeInput{"", "", "", "", "2024-08-06T15:02:00+08:00"}
	// timetest18Expected := timeExpected{time.Time{}, time.Time{}, time.Time{}, time.Time{}, time.Date(2024, 8, 6, 15, 2, 0, 0, time.Local)}
	// transformTest(t, testTimeSourceSchema, transformer, timetest18, timetest18Expected)

	// // default
	// // 2024-08-06T15:02:00Z
	// timetest19 := timeInput{"2024-08-06T15:02:00Z", "", "", "", ""}
	// timetest19Expected := timeExpected{time.Date(2024, 8, 6, 23, 2, 0, 0, time.Local), time.Time{}, time.Time{}, time.Time{}, time.Time{}}
	// transformTest(t, testTimeSourceSchema, transformer, timetest19, timetest19Expected)
	// // 2024-08-06T15:02:00.123Z
	// timetest20 := timeInput{"2024-08-06T15:02:00.123Z", "", "", "", ""}
	// timetest20Expected := timeExpected{time.Date(2024, 8, 6, 23, 2, 0, 123000000, time.Local), time.Time{}, time.Time{}, time.Time{}, time.Time{}}
	// transformTest(t, testTimeSourceSchema, transformer, timetest20, timetest20Expected)
	// // 2024-08-06 15:02:00 (Mysql, Sql Server format)
	// timetest21 := timeInput{"2024-08-06 15:02:00", "", "", "", ""}
	// timetest21Expected := timeExpected{time.Date(2024, 8, 6, 23, 2, 0, 0, time.Local), time.Time{}, time.Time{}, time.Time{}, time.Time{}}
	// transformTest(t, testTimeSourceSchema, transformer, timetest21, timetest21Expected)
	// // 2024-08-06T15:02:00+08:00
	// timetest22 := timeInput{"2024-08-06T15:02:00+08:00", "", "", "", ""}
	// timetest22Expected := timeExpected{time.Date(2024, 8, 6, 15, 2, 0, 0, time.Local), time.Time{}, time.Time{}, time.Time{}, time.Time{}}
	// transformTest(t, testTimeSourceSchema, transformer, timetest22, timetest22Expected)
}

// note: the nano second only exist when you input the string time value
func Test_Time_Precision(t *testing.T) {

	testTimeSourceSchema := schemer.NewSchema()
	err := schemer.UnmarshalJSON([]byte(testTimeSource), testTimeSourceSchema)
	if err != nil {
		t.Error(err)
	}

	// normal case
	// nano second only support when the input value is string
	source := testTimeSourceSchema.Scan(map[string]interface{}{
		"time_second":      1722956520,
		"time_millisecond": 1722956520123,
		"time_microsecond": 1722956520123456,
		"time_nanosecond":  "2024-08-06T15:02:00.123456789Z",
	})

	// properties of time
	assert.Equal(t, time.Unix(1722956520, 0), source.GetValue("time_second").Data.(time.Time))                          // 2024-08-06T15:02:00Z
	assert.Equal(t, time.Unix(1722956520, 123000000), source.GetValue("time_millisecond").Data.(time.Time))             // 2024-08-06T15:02:00.123Z
	assert.Equal(t, time.Unix(1722956520, 123456000), source.GetValue("time_microsecond").Data.(time.Time))             // 2024-08-06T15:02:00.123456Z
	assert.Equal(t, time.Unix(1722956520, 123456789).In(time.UTC), source.GetValue("time_nanosecond").Data.(time.Time)) // 2024-08-06T15:02:00.123456789Z

	// special case
	// todo: try to find the way of testing the wrong precision of nanosecond, currently the time does not support exceeding nanosecond
	source = testTimeSourceSchema.Scan(map[string]interface{}{
		"time_default":     17229565201234,
		"time_second":      1722956520100,
		"time_millisecond": 1722956520123456,
		"time_microsecond": 17229565201234567,
	})

	// properties of time
	assert.NotEqual(t, time.Unix(1722956520, 100000000), source.GetValue("time_second").Data.(time.Time))      // 2024-08-06T15:02:00.1Z
	assert.NotEqual(t, time.Unix(1722956520, 123400000), source.GetValue("time_millisecond").Data.(time.Time)) // 2024-08-06T15:02:00.1234Z
	assert.NotEqual(t, time.Unix(1722956520, 123456700), source.GetValue("time_microsecond").Data.(time.Time)) // 2024-08-06T15:02:00.1234567Z
}

func Test_No_Time_Precision(t *testing.T) {

	testTimeSourceSchema := schemer.NewSchema()
	err := schemer.UnmarshalJSON([]byte(testTimeSource), testTimeSourceSchema)
	if err != nil {
		t.Error(err)
	}

	// normal case
	// nano second only support when the input value is string
	source := testTimeSourceSchema.Scan(map[string]interface{}{
		"time_default": 1722956520123,
	})

	// properties of time
	assert.Equal(t, time.Unix(1722956520, 123000000), source.GetValue("time_default").Data.(time.Time)) // 2024-08-06T15:02:00.123Z

	// special case
	source = testTimeSourceSchema.Scan(map[string]interface{}{
		"time_default": 17229565201234,
	})

	// properties of time
	assert.NotEqual(t, time.Unix(1722956520, 123400000), source.GetValue("time_default").Data.(time.Time)) // 2024-08-06T15:02:00.1234Z
}

func Test_NotSupport_Time_Precision(t *testing.T) {

	testTimeSourceSchema := schemer.NewSchema()
	err := schemer.UnmarshalJSON([]byte(testTimeSource), testTimeSourceSchema)
	if err != nil {
		t.Error(err)
	}

	// normal case
	source_millisecond := testTimeSourceSchema.Scan(map[string]interface{}{
		"time_notSupport": 1722956520123,
	})
	source_microsecond := testTimeSourceSchema.Scan(map[string]interface{}{
		"time_notSupport": 1722956520123456,
	})

	// properties of time
	assert.Equal(t, time.Unix(1722956520, 123000000), source_millisecond.GetValue("time_notSupport").Data.(time.Time))
	assert.Equal(t, time.Unix(1722956520, 123456000), source_microsecond.GetValue("time_notSupport").Data.(time.Time))

	// special case
	source_millisecond = testTimeSourceSchema.Scan(map[string]interface{}{
		"time_notSupport": 17229565201234,
	})
	source_microsecond = testTimeSourceSchema.Scan(map[string]interface{}{
		"time_notSupport": 1722956520123456700,
	})

	// properties of time
	assert.NotEqual(t, time.Unix(1722956520, 123400000), source_millisecond.GetValue("time_notSupport").Data.(time.Time))
	assert.NotEqual(t, time.Unix(1722956520, 123456700), source_microsecond.GetValue("time_notSupport").Data.(time.Time))
}

func Test_Null_Time_Precision(t *testing.T) {

	testTimeSourceSchema := schemer.NewSchema()
	err := schemer.UnmarshalJSON([]byte(testTimeSource), testTimeSourceSchema)
	if err != nil {
		t.Error(err)
	}

	// normal case
	source_millisecond := testTimeSourceSchema.Scan(map[string]interface{}{
		"time_null": 1722956520123,
	})
	source_microsecond := testTimeSourceSchema.Scan(map[string]interface{}{
		"time_null": 1722956520123456,
	})

	// properties of time
	assert.Equal(t, time.Unix(1722956520, 123000000), source_millisecond.GetValue("time_null").Data.(time.Time))
	assert.Equal(t, time.Unix(1722956520, 123456000), source_microsecond.GetValue("time_null").Data.(time.Time))

	// special case
	source_millisecond = testTimeSourceSchema.Scan(map[string]interface{}{
		"time_null": 17229565201234,
	})
	source_microsecond = testTimeSourceSchema.Scan(map[string]interface{}{
		"time_null": 1722956520123456700,
	})

	// properties of time
	assert.NotEqual(t, time.Unix(1722956520, 123400000), source_millisecond.GetValue("time_null").Data.(time.Time))
	assert.NotEqual(t, time.Unix(1722956520, 123456700), source_microsecond.GetValue("time_null").Data.(time.Time))
}

func Test_Abbreviation_Time_Precision(t *testing.T) {

	testTimeSourceSchema := schemer.NewSchema()
	err := schemer.UnmarshalJSON([]byte(testTimeSource), testTimeSourceSchema)
	if err != nil {
		t.Error(err)
	}

	// normal case
	source_millisecond := testTimeSourceSchema.Scan(map[string]interface{}{
		"time_us": 1722956520123,
	})
	source_microsecond := testTimeSourceSchema.Scan(map[string]interface{}{
		"time_us": 1722956520123456,
	})

	// properties of time
	assert.Equal(t, time.Unix(1722956520, 123000000), source_millisecond.GetValue("time_us").Data.(time.Time))
	assert.Equal(t, time.Unix(1722956520, 123456000), source_microsecond.GetValue("time_us").Data.(time.Time))

	// special case
	source_millisecond = testTimeSourceSchema.Scan(map[string]interface{}{
		"time_us": 17229565201234,
	})
	source_microsecond = testTimeSourceSchema.Scan(map[string]interface{}{
		"time_us": 1722956520123456700,
	})

	// properties of time
	assert.NotEqual(t, time.Unix(1722956520, 123400000), source_millisecond.GetValue("time_us").Data.(time.Time))
	assert.NotEqual(t, time.Unix(1722956520, 123456700), source_microsecond.GetValue("time_us").Data.(time.Time))
}

func Test_MixedCase_Time_Precision(t *testing.T) {

	testTimeSourceSchema := schemer.NewSchema()
	err := schemer.UnmarshalJSON([]byte(testTimeSource), testTimeSourceSchema)
	if err != nil {
		t.Error(err)
	}

	// normal case
	source_millisecond := testTimeSourceSchema.Scan(map[string]interface{}{
		"time_MICROSecond": 1722956520123,
	})
	source_microsecond := testTimeSourceSchema.Scan(map[string]interface{}{
		"time_MICROSecond": 1722956520123456,
	})

	// properties of time
	assert.Equal(t, time.Unix(1722956520, 123000000), source_millisecond.GetValue("time_MICROSecond").Data.(time.Time))
	assert.Equal(t, time.Unix(1722956520, 123456000), source_microsecond.GetValue("time_MICROSecond").Data.(time.Time))

	// special case
	source_millisecond = testTimeSourceSchema.Scan(map[string]interface{}{
		"time_MICROSecond": 17229565201234,
	})
	source_microsecond = testTimeSourceSchema.Scan(map[string]interface{}{
		"time_MICROSecond": 1722956520123456700,
	})

	// properties of time
	assert.NotEqual(t, time.Unix(1722956520, 123400000), source_millisecond.GetValue("time_MICROSecond").Data.(time.Time))
	assert.NotEqual(t, time.Unix(1722956520, 123456700), source_microsecond.GetValue("time_MICROSecond").Data.(time.Time))
}

func Test_Time_GetValue_with_timeTime(t *testing.T) {
	testTimeSourceSchema := schemer.NewSchema()
	err := schemer.UnmarshalJSON([]byte(testTimeSource), testTimeSourceSchema)
	if err != nil {
		t.Error(err)
	}

	// normal case
	// nano second only support when the input value is string
	source := testTimeSourceSchema.Scan(map[string]interface{}{
		"time_second":      time.Unix(1722956520, 0),
		"time_millisecond": time.Unix(1722956520, 123000000),
		"time_microsecond": time.Unix(1722956520, 123456000),
		"time_default":     time.Unix(1722956520, 123000000),
		"time_notSupport":  time.Unix(1722956520, 123000000),
		"time_null":        time.Unix(1722956520, 123000000),
		"time_us":          time.Unix(1722956520, 123000000),
		"time_MICROSecond": time.Unix(1722956520, 123000000),
	})

	// properties of time
	assert.Equal(t, time.Unix(1722956520, 0), source.GetValue("time_second").Data.(time.Time))              // 2024-08-06T15:02:00Z
	assert.Equal(t, time.Unix(1722956520, 123000000), source.GetValue("time_millisecond").Data.(time.Time)) // 2024-08-06T15:02:00.123Z
	assert.Equal(t, time.Unix(1722956520, 123456000), source.GetValue("time_microsecond").Data.(time.Time)) // 2024-08-06T15:02:00.123456Z
	assert.Equal(t, time.Unix(1722956520, 123000000), source.GetValue("time_default").Data.(time.Time))     // 2024-08-06T15:02:00.123Z
	assert.Equal(t, time.Unix(1722956520, 123000000), source.GetValue("time_notSupport").Data.(time.Time))  // 2024-08-06T15:02:00.123Z
	assert.Equal(t, time.Unix(1722956520, 123000000), source.GetValue("time_null").Data.(time.Time))        // 2024-08-06T15:02:00.123Z
	assert.Equal(t, time.Unix(1722956520, 123000000), source.GetValue("time_us").Data.(time.Time))          // 2024-08-06T15:02:00.123Z
	assert.Equal(t, time.Unix(1722956520, 123000000), source.GetValue("time_MICROSecond").Data.(time.Time)) // 2024-08-06T15:02:00.123Z

	// special case
	source = testTimeSourceSchema.Scan(map[string]interface{}{
		"time_second":      time.Unix(1722956520, 100000000),
		"time_millisecond": time.Unix(1722956520, 123400000),
		"time_microsecond": time.Unix(1722956520, 123456700),
		"time_default":     time.Unix(1722956520, 123400000),
		"time_notSupport":  time.Unix(1722956520, 123400000),
		"time_null":        time.Unix(1722956520, 123400000),
		"time_us":          time.Unix(1722956520, 123400000),
		"time_MICROSecond": time.Unix(1722956520, 123400000),
	})

	// properties of time
	assert.Equal(t, time.Unix(1722956520, 100000000), source.GetValue("time_second").Data.(time.Time))      // 2024-08-06T15:02:00.1Z
	assert.Equal(t, time.Unix(1722956520, 123400000), source.GetValue("time_millisecond").Data.(time.Time)) // 2024-08-06T15:02:00.1234Z
	assert.Equal(t, time.Unix(1722956520, 123456700), source.GetValue("time_microsecond").Data.(time.Time)) // 2024-08-06T15:02:00.1234567Z
	assert.Equal(t, time.Unix(1722956520, 123400000), source.GetValue("time_default").Data.(time.Time))     // 2024-08-06T15:02:00.1234Z
	assert.Equal(t, time.Unix(1722956520, 123400000), source.GetValue("time_notSupport").Data.(time.Time))  // 2024-08-06T15:02:00.1234Z
	assert.Equal(t, time.Unix(1722956520, 123400000), source.GetValue("time_null").Data.(time.Time))        // 2024-08-06T15:02:00.1234Z
	assert.Equal(t, time.Unix(1722956520, 123400000), source.GetValue("time_us").Data.(time.Time))          // 2024-08-06T15:02:00.1234Z
	assert.Equal(t, time.Unix(1722956520, 123400000), source.GetValue("time_MICROSecond").Data.(time.Time)) // 2024-08-06T15:02:00.1234Z
}

func Test_Time_GetValue_with_uint64(t *testing.T) {
	testTimeSourceSchema := schemer.NewSchema()
	err := schemer.UnmarshalJSON([]byte(testTimeSource), testTimeSourceSchema)
	if err != nil {
		t.Error(err)
	}

	// normal case
	source := testTimeSourceSchema.Scan(map[string]interface{}{
		"time_second":      uint64(1722956520),
		"time_millisecond": uint64(1722956520123),
		"time_microsecond": uint64(1722956520123456),
		"time_default":     uint64(1722956520123),
		"time_notSupport":  uint64(1722956520123),
		"time_null":        uint64(1722956520123),
		"time_us":          uint64(1722956520123),
		"time_MICROSecond": uint64(1722956520123),
	})

	// properties of time
	assert.Equal(t, time.Unix(1722956520, 0), source.GetValue("time_second").Data.(time.Time))              // 2024-08-06T15:02:00Z
	assert.Equal(t, time.Unix(1722956520, 123000000), source.GetValue("time_millisecond").Data.(time.Time)) // 2024-08-06T15:02:00.123Z
	assert.Equal(t, time.Unix(1722956520, 123456000), source.GetValue("time_microsecond").Data.(time.Time)) // 2024-08-06T15:02:00.123456Z
	assert.Equal(t, time.Unix(1722956520, 123000000), source.GetValue("time_default").Data.(time.Time))     // 2024-08-06T15:02:00.123Z
	assert.Equal(t, time.Unix(1722956520, 123000000), source.GetValue("time_notSupport").Data.(time.Time))  // 2024-08-06T15:02:00.123Z
	assert.Equal(t, time.Unix(1722956520, 123000000), source.GetValue("time_null").Data.(time.Time))        // 2024-08-06T15:02:00.123Z
	assert.Equal(t, time.Unix(1722956520, 123000000), source.GetValue("time_us").Data.(time.Time))          // 2024-08-06T15:02:00.123Z
	assert.Equal(t, time.Unix(1722956520, 123000000), source.GetValue("time_MICROSecond").Data.(time.Time)) // 2024-08-06T15:02:00.123Z

	// special case
	source = testTimeSourceSchema.Scan(map[string]interface{}{
		"time_second":      uint64(1722956520100),
		"time_millisecond": uint64(17229565201234),
		"time_microsecond": uint64(17229565201234567),
		"time_default":     uint64(17229565201234),
		"time_notSupport":  uint64(17229565201234),
		"time_null":        uint64(17229565201234),
		"time_us":          uint64(17229565201234),
		"time_MICROSecond": uint64(17229565201234),
	})

	// properties of time
	assert.NotEqual(t, time.Unix(1722956520, 100000000), source.GetValue("time_second").Data.(time.Time))      // 2024-08-06T15:02:00.1Z
	assert.NotEqual(t, time.Unix(1722956520, 123400000), source.GetValue("time_millisecond").Data.(time.Time)) // 2024-08-06T15:02:00.1234Z
	assert.NotEqual(t, time.Unix(1722956520, 123456700), source.GetValue("time_microsecond").Data.(time.Time)) // 2024-08-06T15:02:00.1234567Z
	assert.NotEqual(t, time.Unix(1722956520, 123400000), source.GetValue("time_default").Data.(time.Time))     // 2024-08-06T15:02:00.1234Z
	assert.NotEqual(t, time.Unix(1722956520, 123400000), source.GetValue("time_notSupport").Data.(time.Time))  // 2024-08-06T15:02:00.1234Z
	assert.NotEqual(t, time.Unix(1722956520, 123400000), source.GetValue("time_null").Data.(time.Time))        // 2024-08-06T15:02:00.1234Z
	assert.NotEqual(t, time.Unix(1722956520, 123400000), source.GetValue("time_us").Data.(time.Time))          // 2024-08-06T15:02:00.1234Z
	assert.NotEqual(t, time.Unix(1722956520, 123400000), source.GetValue("time_MICROSecond").Data.(time.Time)) // 2024-08-06T15:02:00.1234Z
}

func Test_Time_GetValue_with_string(t *testing.T) {
	testTimeSourceSchema := schemer.NewSchema()
	err := schemer.UnmarshalJSON([]byte(testTimeSource), testTimeSourceSchema)
	if err != nil {
		t.Error(err)
	}

	// normal case
	source := testTimeSourceSchema.Scan(map[string]interface{}{
		"time_second":      "2024-08-06T15:02:00Z",
		"time_millisecond": "2024-08-06T15:02:00.123Z",
		"time_microsecond": "2024-08-06T15:02:00.123456Z",
		"time_default":     "2024-08-06T15:02:00.123Z",
		"time_notSupport":  "2024-08-06T15:02:00.123Z",
		"time_null":        "2024-08-06T15:02:00.123Z",
		"time_us":          "2024-08-06T15:02:00.123Z",
		"time_MICROSecond": "2024-08-06T15:02:00.123Z",
	})

	// properties of time
	assert.Equal(t, time.Unix(1722956520, 0).In(time.UTC), source.GetValue("time_second").Data.(time.Time))              // 2024-08-06T15:02:00Z
	assert.Equal(t, time.Unix(1722956520, 123000000).In(time.UTC), source.GetValue("time_millisecond").Data.(time.Time)) // 2024-08-06T15:02:00.123Z
	assert.Equal(t, time.Unix(1722956520, 123456000).In(time.UTC), source.GetValue("time_microsecond").Data.(time.Time)) // 2024-08-06T15:02:00.123456Z
	assert.Equal(t, time.Unix(1722956520, 123000000).In(time.UTC), source.GetValue("time_default").Data.(time.Time))     // 2024-08-06T15:02:00.123Z
	assert.Equal(t, time.Unix(1722956520, 123000000).In(time.UTC), source.GetValue("time_notSupport").Data.(time.Time))  // 2024-08-06T15:02:00.123Z
	assert.Equal(t, time.Unix(1722956520, 123000000).In(time.UTC), source.GetValue("time_null").Data.(time.Time))        // 2024-08-06T15:02:00.123Z
	assert.Equal(t, time.Unix(1722956520, 123000000).In(time.UTC), source.GetValue("time_us").Data.(time.Time))          // 2024-08-06T15:02:00.123Z
	assert.Equal(t, time.Unix(1722956520, 123000000).In(time.UTC), source.GetValue("time_MICROSecond").Data.(time.Time)) // 2024-08-06T15:02:00.123Z

	// special case
	source = testTimeSourceSchema.Scan(map[string]interface{}{
		"time_second":      "2024-08-06T15:02:00.1Z",
		"time_millisecond": "2024-08-06T15:02:00.1234Z",
		"time_microsecond": "2024-08-06T15:02:00.1234567Z",
		"time_default":     "2024-08-06T15:02:00.1234Z",
		"time_notSupport":  "2024-08-06T15:02:00.1234Z",
		"time_null":        "2024-08-06T15:02:00.1234Z",
		"time_us":          "2024-08-06T15:02:00.1234Z",
		"time_MICROSecond": "2024-08-06T15:02:00.1234Z",
	})

	// properties of time
	assert.Equal(t, time.Unix(1722956520, 100000000).In(time.UTC), source.GetValue("time_second").Data.(time.Time))      // 2024-08-06T15:02:00.1Z
	assert.Equal(t, time.Unix(1722956520, 123400000).In(time.UTC), source.GetValue("time_millisecond").Data.(time.Time)) // 2024-08-06T15:02:00.1234Z
	assert.Equal(t, time.Unix(1722956520, 123456700).In(time.UTC), source.GetValue("time_microsecond").Data.(time.Time)) // 2024-08-06T15:02:00.1234567Z
	assert.Equal(t, time.Unix(1722956520, 123400000).In(time.UTC), source.GetValue("time_default").Data.(time.Time))     // 2024-08-06T15:02:00.1234Z
	assert.Equal(t, time.Unix(1722956520, 123400000).In(time.UTC), source.GetValue("time_notSupport").Data.(time.Time))  // 2024-08-06T15:02:00.1234Z
	assert.Equal(t, time.Unix(1722956520, 123400000).In(time.UTC), source.GetValue("time_null").Data.(time.Time))        // 2024-08-06T15:02:00.1234Z
	assert.Equal(t, time.Unix(1722956520, 123400000).In(time.UTC), source.GetValue("time_us").Data.(time.Time))          // 2024-08-06T15:02:00.1234Z
	assert.Equal(t, time.Unix(1722956520, 123400000).In(time.UTC), source.GetValue("time_MICROSecond").Data.(time.Time)) // 2024-08-06T15:02:00.1234Z
}
func Test_Time_GetValue_with_float64(t *testing.T) {
	testTimeSourceSchema := schemer.NewSchema()
	err := schemer.UnmarshalJSON([]byte(testTimeSource), testTimeSourceSchema)
	if err != nil {
		t.Error(err)
	}

	// normal case
	source := testTimeSourceSchema.Scan(map[string]interface{}{
		"time_second":      float64(1722956520),
		"time_millisecond": float64(1722956520123),
		"time_microsecond": float64(1722956520123456),
		"time_default":     float64(1722956520123),
		"time_notSupport":  float64(1722956520123),
		"time_null":        float64(1722956520123),
		"time_us":          float64(1722956520123),
		"time_MICROSecond": float64(1722956520123),
	})

	// properties of time
	assert.Equal(t, time.Unix(1722956520, 0), source.GetValue("time_second").Data.(time.Time))              // 2024-08-06T15:02:00Z
	assert.Equal(t, time.Unix(1722956520, 123000000), source.GetValue("time_millisecond").Data.(time.Time)) // 2024-08-06T15:02:00.123Z
	assert.Equal(t, time.Unix(1722956520, 123456000), source.GetValue("time_microsecond").Data.(time.Time)) // 2024-08-06T15:02:00.123456Z
	assert.Equal(t, time.Unix(1722956520, 123000000), source.GetValue("time_default").Data.(time.Time))     // 2024-08-06T15:02:00.123Z
	assert.Equal(t, time.Unix(1722956520, 123000000), source.GetValue("time_notSupport").Data.(time.Time))  // 2024-08-06T15:02:00.123Z
	assert.Equal(t, time.Unix(1722956520, 123000000), source.GetValue("time_null").Data.(time.Time))        // 2024-08-06T15:02:00.123Z
	assert.Equal(t, time.Unix(1722956520, 123000000), source.GetValue("time_us").Data.(time.Time))          // 2024-08-06T15:02:00.123Z
	assert.Equal(t, time.Unix(1722956520, 123000000), source.GetValue("time_MICROSecond").Data.(time.Time)) // 2024-08-06T15:02:00.123Z

	// special case
	source = testTimeSourceSchema.Scan(map[string]interface{}{
		"time_second":      float64(1722956520100),
		"time_millisecond": float64(17229565201234),
		"time_microsecond": float64(17229565201234567),
		"time_default":     float64(17229565201234),
		"time_notSupport":  float64(17229565201234),
		"time_null":        float64(17229565201234),
		"time_us":          float64(17229565201234),
		"time_MICROSecond": float64(17229565201234),
	})

	// properties of time
	assert.NotEqual(t, time.Unix(1722956520, 100000000), source.GetValue("time_second").Data.(time.Time))      // 2024-08-06T15:02:00.1Z
	assert.NotEqual(t, time.Unix(1722956520, 123400000), source.GetValue("time_millisecond").Data.(time.Time)) // 2024-08-06T15:02:00.1234Z
	assert.NotEqual(t, time.Unix(1722956520, 123456700), source.GetValue("time_microsecond").Data.(time.Time)) // 2024-08-06T15:02:00.1234567Z
	assert.NotEqual(t, time.Unix(1722956520, 123400000), source.GetValue("time_default").Data.(time.Time))     // 2024-08-06T15:02:00.1234Z
	assert.NotEqual(t, time.Unix(1722956520, 123400000), source.GetValue("time_notSupport").Data.(time.Time))  // 2024-08-06T15:02:00.1234Z
	assert.NotEqual(t, time.Unix(1722956520, 123400000), source.GetValue("time_null").Data.(time.Time))        // 2024-08-06T15:02:00.1234Z
	assert.NotEqual(t, time.Unix(1722956520, 123400000), source.GetValue("time_us").Data.(time.Time))          // 2024-08-06T15:02:00.1234Z
	assert.NotEqual(t, time.Unix(1722956520, 123400000), source.GetValue("time_MICROSecond").Data.(time.Time)) // 2024-08-06T15:02:00.1234Z
}
