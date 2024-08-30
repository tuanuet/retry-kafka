package marshaler

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestByteMarshaler(t *testing.T) {
	bm := ByteMarshaler{}
	jsonString := `{"name":"tuan"}`
	bytes, err := bm.Marshal([]byte(jsonString))
	assert.NoError(t, err)
	assert.Equal(t, jsonString, string(bytes))

	actual := make([]byte, 0)
	err = bm.Unmarshal(bytes, &actual)
	assert.NoError(t, err)
	assert.Equal(t, []byte(jsonString), actual)
}
