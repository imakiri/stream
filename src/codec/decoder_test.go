package codec

import (
	"context"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

const testSession = 0x5e55_1011

func TestDispatcher(t *testing.T) {
	var decoder, err = NewDecoder(4)
	assert.NoError(t, err)

	var ctx, cancel = context.WithCancel(context.Background())
	defer cancel()

	decoder.resultQueue = make(chan DecodedChunk, 8)
	decoder.dispatchQueue = make(chan dispatchItem, 4)
	go decoder.dispatch(ctx)

	var chunk *Chunk
	var data []byte

	chunk = NewChunk(1, KindData, []byte(strconv.Itoa(1)))
	decoder.dispatchQueue <- dispatchItem{chunk: chunk}
	t.Log("sent chunk: data ", string(chunk.Data()))

	chunk = NewChunk(2, KindData, []byte(strconv.Itoa(2)))
	decoder.dispatchQueue <- dispatchItem{chunk: chunk}
	t.Log("sent chunk: data ", string(chunk.Data()))

	chunk = NewChunk(3, KindData, []byte(strconv.Itoa(3)))
	decoder.dispatchQueue <- dispatchItem{chunk: chunk}
	t.Log("sent chunk: data ", string(chunk.Data()))

	chunk = NewChunk(4, KindData, []byte(strconv.Itoa(4)))
	decoder.dispatchQueue <- dispatchItem{chunk: chunk}
	t.Log("sent chunk: data ", string(chunk.Data()))

	chunk = NewChunk(5, KindData, []byte(strconv.Itoa(5)))
	decoder.dispatchQueue <- dispatchItem{chunk: chunk}
	t.Log("sent chunk: data ", string(chunk.Data()))

	data = (<-decoder.resultQueue).Data
	t.Log("received chunk: data ", string(data))
	assert.EqualValues(t, []byte(strconv.Itoa(1)), data)

	data = (<-decoder.resultQueue).Data
	t.Log("received chunk: data ", string(data))
	assert.EqualValues(t, []byte(strconv.Itoa(2)), data)

	data = (<-decoder.resultQueue).Data
	t.Log("received chunk: data ", string(data))
	assert.EqualValues(t, []byte(strconv.Itoa(3)), data)

	data = (<-decoder.resultQueue).Data
	t.Log("received chunk: data ", string(data))
	assert.EqualValues(t, []byte(strconv.Itoa(4)), data)

	data = (<-decoder.resultQueue).Data
	t.Log("received chunk: data ", string(data))
	assert.EqualValues(t, []byte(strconv.Itoa(5)), data)

	//

	chunk = NewChunk(6, KindData, []byte(strconv.Itoa(6)))
	t.Log("sent chunk: data ", string(chunk.Data()))
	decoder.dispatchQueue <- dispatchItem{chunk: chunk}

	chunk = NewChunk(8, KindData, []byte(strconv.Itoa(8)))
	t.Log("sent chunk: data ", string(chunk.Data()))
	decoder.dispatchQueue <- dispatchItem{chunk: chunk}

	chunk = NewChunk(10, KindData, []byte(strconv.Itoa(10)))
	t.Log("sent chunk: data ", string(chunk.Data()))
	decoder.dispatchQueue <- dispatchItem{chunk: chunk}

	chunk = NewChunk(7, KindData, []byte(strconv.Itoa(7)))
	t.Log("sent chunk: data ", string(chunk.Data()))
	decoder.dispatchQueue <- dispatchItem{chunk: chunk}

	chunk = NewChunk(9, KindData, []byte(strconv.Itoa(9)))
	t.Log("sent chunk: data ", string(chunk.Data()))
	decoder.dispatchQueue <- dispatchItem{chunk: chunk}

	data = (<-decoder.resultQueue).Data
	t.Log("received chunk: data ", string(data))
	assert.EqualValues(t, []byte(strconv.Itoa(6)), data)

	data = (<-decoder.resultQueue).Data
	t.Log("received chunk: data ", string(data))
	assert.EqualValues(t, []byte(strconv.Itoa(7)), data)

	data = (<-decoder.resultQueue).Data
	t.Log("received chunk: data ", string(data))
	assert.EqualValues(t, []byte(strconv.Itoa(8)), data)

	data = (<-decoder.resultQueue).Data
	t.Log("received chunk: data ", string(data))
	assert.EqualValues(t, []byte(strconv.Itoa(9)), data)

	data = (<-decoder.resultQueue).Data
	t.Log("received chunk: data ", string(data))
	assert.EqualValues(t, []byte(strconv.Itoa(10)), data)

	//

	chunk = NewChunk(12, KindData, []byte(strconv.Itoa(12)))
	t.Log("sent chunk: data ", string(chunk.Data()))
	decoder.dispatchQueue <- dispatchItem{chunk: chunk}

	chunk = NewChunk(13, KindData, []byte(strconv.Itoa(13)))
	t.Log("sent chunk: data ", string(chunk.Data()))
	decoder.dispatchQueue <- dispatchItem{chunk: chunk}

	chunk = NewChunk(14, KindData, []byte(strconv.Itoa(14)))
	t.Log("sent chunk: data ", string(chunk.Data()))
	decoder.dispatchQueue <- dispatchItem{chunk: chunk}

	chunk = NewChunk(15, KindData, []byte(strconv.Itoa(15)))
	t.Log("sent chunk: data ", string(chunk.Data()))
	decoder.dispatchQueue <- dispatchItem{chunk: chunk}

	chunk = NewChunk(16, KindData, []byte(strconv.Itoa(16)))
	t.Log("sent chunk: data ", string(chunk.Data()))
	decoder.dispatchQueue <- dispatchItem{chunk: chunk}

	chunk = NewChunk(11, KindData, []byte(strconv.Itoa(11)))
	t.Log("sent chunk: data ", string(chunk.Data()))
	decoder.dispatchQueue <- dispatchItem{chunk: chunk}

	data = (<-decoder.resultQueue).Data
	t.Log("received chunk: data ", string(data))
	assert.EqualValues(t, []byte(strconv.Itoa(12)), data)

	data = (<-decoder.resultQueue).Data
	t.Log("received chunk: data ", string(data))
	assert.EqualValues(t, []byte(strconv.Itoa(13)), data)

	data = (<-decoder.resultQueue).Data
	t.Log("received chunk: data ", string(data))
	assert.EqualValues(t, []byte(strconv.Itoa(14)), data)

	data = (<-decoder.resultQueue).Data
	t.Log("received chunk: data ", string(data))
	assert.EqualValues(t, []byte(strconv.Itoa(15)), data)

	data = (<-decoder.resultQueue).Data
	t.Log("received chunk: data ", string(data))
	assert.EqualValues(t, []byte(strconv.Itoa(16)), data)
}
