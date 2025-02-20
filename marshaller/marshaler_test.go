package marshaller

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestByteMarshaller(t *testing.T) {
	bm := ByteMarshaller{}
	jsonString := `{"name":"tuan"}`
	bytes, err := bm.Marshal([]byte(jsonString))
	assert.NoError(t, err)
	assert.Equal(t, jsonString, string(bytes))

	actual := make([]byte, 0)
	err = bm.Unmarshal(bytes, &actual)
	assert.NoError(t, err)
	assert.Equal(t, []byte(jsonString), actual)
}
