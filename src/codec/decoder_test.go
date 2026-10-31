package codec

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"strconv"
	"testing"
)

const testSession = 0x5e55_1011

func requirePackets(t *testing.T, packets []*PacketV2, csn uint64, expect map[uint64]map[uint64][]byte) {
	var expectPackets, ok = expect[csn]
	if !ok {
		require.Fail(t, "invalid test data")
		t.Fatal()
	}

	for i := range packets {
		var value, ok = expectPackets[uint64(i)]
		if ok && packets[i] == nil {
			require.Fail(t, fmt.Sprintf("expect packet at csn: %d, psn: %d", csn, i), packets)
			t.Fatal()
		}
		if !ok && packets[i] != nil {
			require.Fail(t, fmt.Sprintf("did not expect packet at csn: %d, psn: %d", csn, i), packets)
			t.Fatal()
		}
		if !ok && packets[i] == nil {
			continue
		}
		if ok && packets[i] != nil {
			require.EqualValues(t, packets[i].csn, csn)
			for j := range value {
				require.EqualValues(t, value[j], packets[i].data[j])
			}
			continue
		}
	}
}

func casePacket(csn, psn uint64, expect map[uint64]map[uint64][]byte) *PacketV2 {
	var data = []byte(fmt.Sprintf("%d.%d", csn, psn))
	var packet, _ = NewPacketV2(KindData, csn, uint32(psn), testSession, data)
	if expect[csn] == nil {
		expect[csn] = make(map[uint64][]byte)
	}
	expect[csn][psn] = data
	return packet
}

func TestAssembler(t *testing.T) {
	var decoder, err = NewDecoder(4, 6, 3, 4)
	require.NoError(t, err)

	var ctx, cancel = context.WithCancel(context.Background())
	defer cancel()

	decoder.restoreQueue = make(chan []*PacketV2, 4)
	decoder.assemblyQueue = make(chan *PacketV2, 8)
	go decoder.assembly(ctx)

	var packets []*PacketV2
	var expect = make(map[uint64]map[uint64][]byte)

	decoder.assemblyQueue <- casePacket(1, 0, expect)
	decoder.assemblyQueue <- casePacket(1, 2, expect)
	decoder.assemblyQueue <- casePacket(1, 3, expect)
	decoder.assemblyQueue <- casePacket(1, 4, expect)

	packets = <-decoder.restoreQueue
	requirePackets(t, packets, 1, expect)

	decoder.assemblyQueue <- casePacket(2, 0, expect)
	decoder.assemblyQueue <- casePacket(3, 1, expect)
	decoder.assemblyQueue <- casePacket(2, 2, expect)
	decoder.assemblyQueue <- casePacket(3, 2, expect)
	decoder.assemblyQueue <- casePacket(2, 3, expect)
	decoder.assemblyQueue <- casePacket(3, 5, expect)
	decoder.assemblyQueue <- casePacket(2, 4, expect)
	decoder.assemblyQueue <- casePacket(3, 3, expect)

	packets = <-decoder.restoreQueue
	requirePackets(t, packets, 2, expect)

	packets = <-decoder.restoreQueue
	requirePackets(t, packets, 3, expect)

	decoder.assemblyQueue <- casePacket(4, 0, expect)
	decoder.assemblyQueue <- casePacket(5, 1, expect)
	decoder.assemblyQueue <- casePacket(6, 2, expect)
	decoder.assemblyQueue <- casePacket(7, 2, expect)
	decoder.assemblyQueue <- casePacket(8, 3, expect)
	decoder.assemblyQueue <- casePacket(9, 5, expect)
	decoder.assemblyQueue <- casePacket(9, 4, expect)
	decoder.assemblyQueue <- casePacket(9, 3, expect)
	decoder.assemblyQueue <- casePacket(9, 1, expect)

	packets = <-decoder.restoreQueue
	requirePackets(t, packets, 9, expect)
}

func TestRestorer(t *testing.T) {
	const dataParts = 4
	const totalParts = 6
	var decoder, err = NewDecoder(dataParts, totalParts, 3, 4)
	require.NoError(t, err)

	var ctx, cancel = context.WithCancel(context.Background())
	defer cancel()

	decoder.restoreQueue = make(chan []*PacketV2, 4)
	decoder.dispatchQueue = make(chan *Chunk, 4)
	go decoder.restore(ctx)

	var chunkExpected *Chunk
	var buf [][]byte
	var rem = []byte{}
	var chunkGot *Chunk
	var packet *PacketV2
	var packets = make([]*PacketV2, totalParts)

	chunkExpected, _ = NewChunk(4, 1, KindData, []byte("123"))
	buf = chunkExpected.Marshal(6)

	err = decoder.restorer.rs.Encode(buf)
	require.NoError(t, err)

	for i := range buf {
		packet, rem = NewPacketV2(KindData, 1, uint32(i), testSession, buf[i])
		assert.Nil(t, rem)
		packets[i] = packet
	}

	decoder.restoreQueue <- packets
	chunkGot = <-decoder.dispatchQueue
	assert.NotNil(t, chunkGot)
	assert.EqualValues(t, 1, chunkGot.csn)

	//

	chunkExpected, _ = NewChunk(4, 2, KindData, []byte("123"))
	buf = chunkExpected.Marshal(6)

	err = decoder.restorer.rs.Encode(buf)
	assert.NoError(t, err)

	for i := range buf {
		packet, rem = NewPacketV2(KindData, 2, uint32(i), testSession, buf[i])
		assert.Nil(t, rem)
		packets[i] = packet
	}

	packets[0] = nil
	packets[5] = nil

	decoder.restoreQueue <- packets
	chunkGot = <-decoder.dispatchQueue
	assert.NotNil(t, chunkGot)
	assert.EqualValues(t, 2, chunkGot.csn)
}

func TestDispatcher(t *testing.T) {
	var decoder, err = NewDecoder(8, 12, 8, 4)
	assert.NoError(t, err)

	var ctx, cancel = context.WithCancel(context.Background())
	defer cancel()

	decoder.resultQueue = make(chan DecodedChunk, 8)
	decoder.dispatchQueue = make(chan *Chunk, 4)
	go decoder.dispatch(ctx)

	var chunk *Chunk
	var data []byte

	chunk, _ = NewChunk(8, 1, KindData, []byte(strconv.Itoa(1)))
	decoder.dispatchQueue <- chunk
	t.Log("sent chunk: data ", string(chunk.Data()))

	chunk, _ = NewChunk(8, 2, KindData, []byte(strconv.Itoa(2)))
	decoder.dispatchQueue <- chunk
	t.Log("sent chunk: data ", string(chunk.Data()))

	chunk, _ = NewChunk(8, 3, KindData, []byte(strconv.Itoa(3)))
	decoder.dispatchQueue <- chunk
	t.Log("sent chunk: data ", string(chunk.Data()))

	chunk, _ = NewChunk(8, 4, KindData, []byte(strconv.Itoa(4)))
	decoder.dispatchQueue <- chunk
	t.Log("sent chunk: data ", string(chunk.Data()))

	chunk, _ = NewChunk(8, 5, KindData, []byte(strconv.Itoa(5)))
	decoder.dispatchQueue <- chunk
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

	chunk, _ = NewChunk(8, 6, KindData, []byte(strconv.Itoa(6)))
	t.Log("sent chunk: data ", string(chunk.Data()))
	decoder.dispatchQueue <- chunk

	chunk, _ = NewChunk(8, 8, KindData, []byte(strconv.Itoa(8)))
	t.Log("sent chunk: data ", string(chunk.Data()))
	decoder.dispatchQueue <- chunk

	chunk, _ = NewChunk(8, 10, KindData, []byte(strconv.Itoa(10)))
	t.Log("sent chunk: data ", string(chunk.Data()))
	decoder.dispatchQueue <- chunk

	chunk, _ = NewChunk(8, 7, KindData, []byte(strconv.Itoa(7)))
	t.Log("sent chunk: data ", string(chunk.Data()))
	decoder.dispatchQueue <- chunk

	chunk, _ = NewChunk(8, 9, KindData, []byte(strconv.Itoa(9)))
	t.Log("sent chunk: data ", string(chunk.Data()))
	decoder.dispatchQueue <- chunk

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

	chunk, _ = NewChunk(8, 12, KindData, []byte(strconv.Itoa(12)))
	t.Log("sent chunk: data ", string(chunk.Data()))
	decoder.dispatchQueue <- chunk

	chunk, _ = NewChunk(8, 13, KindData, []byte(strconv.Itoa(13)))
	t.Log("sent chunk: data ", string(chunk.Data()))
	decoder.dispatchQueue <- chunk

	chunk, _ = NewChunk(8, 14, KindData, []byte(strconv.Itoa(14)))
	t.Log("sent chunk: data ", string(chunk.Data()))
	decoder.dispatchQueue <- chunk

	chunk, _ = NewChunk(8, 15, KindData, []byte(strconv.Itoa(15)))
	t.Log("sent chunk: data ", string(chunk.Data()))
	decoder.dispatchQueue <- chunk

	chunk, _ = NewChunk(8, 16, KindData, []byte(strconv.Itoa(16)))
	t.Log("sent chunk: data ", string(chunk.Data()))
	decoder.dispatchQueue <- chunk

	chunk, _ = NewChunk(8, 11, KindData, []byte(strconv.Itoa(11)))
	t.Log("sent chunk: data ", string(chunk.Data()))
	decoder.dispatchQueue <- chunk

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
