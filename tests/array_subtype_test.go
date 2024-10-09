package tests

import (
	"testing"

	"github.com/BrobridgeOrg/schemer"
	"github.com/stretchr/testify/assert"
)

var arraySchema2 = `{
    "array_null":{
        "type":"array",
        "subtype":""
    },
    "array_space":{
        "type":"array",
        "subtype":" "
    },
    "array_abc":{
        "type":"array",
        "subtype":"abc"
    },
    "array_chinese":{
        "type":"array",
        "subtype":"中文"
    },
    "array_special":{
        "type":"array",
        "subtype":"!@#$%^&*()_+{}:<>?~-=[]\\;',./"
    },
    "array_maxLen":{
        "type":"array",
        "subtype":"[max_len_str()]"
    }
}`

func Test_WorngSubtypeWithString(t *testing.T) {

	testSourceSchema := schemer.NewSchema()
	err := schemer.UnmarshalJSON([]byte(arraySchema2), testSourceSchema)
	assert.Error(t, err)
}
