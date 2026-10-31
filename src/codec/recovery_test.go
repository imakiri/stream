package codec

import (
	"bytes"
	"context"
	"crypto/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const (
	recoveryDataParts  = 8
	recoveryTotalParts = 10
	recoveryGroup      = 4 // encoder dispatcher size: chunks interleaved together
)

// recoveryRun encodes chunks, passes every packet the drop function keeps to
// the decoder and returns the decoded chunks in order, together with the
// chunks that went into the encoder.
func recoveryRun(t *testing.T, ctx context.Context, decoderIn chan<- []byte, decoderOut <-chan DecodedChunk,
	chunks int, drop func(p *PacketV2) bool) (sent, got [][]byte) {
	t.Helper()
	encoder, err := NewEncoder(5*time.Millisecond, recoveryGroup, recoveryDataParts, recoveryTotalParts)
	require.NoError(t, err)
	encoderCtx, stopEncoder := context.WithCancel(ctx)
	defer stopEncoder()
	encoderIn, encoderOut, err := encoder.Encode(encoderCtx)
	require.NoError(t, err)

	for range chunks {
		var b = make([]byte, encoder.IncomingSize())
		_, _ = rand.Read(b)
		sent = append(sent, b)
	}
	go func() {
		for _, b := range sent {
			encoderIn <- b
		}
	}()
	go func() {
		for range chunks * recoveryTotalParts {
			var raw = <-encoderOut
			var p PacketV2
			if !p.Unmarshal(raw) {
				t.Error("encoder produced an invalid packet")
				return
			}
			if !drop(&p) {
				decoderIn <- raw
			}
		}
	}()

	for range chunks {
		select {
		case b := <-decoderOut:
			got = append(got, b.Data)
		case <-time.After(time.Second):
			return sent, got
		}
	}
	return sent, got
}

// TestDecoderRecoversLossInEveryChunkOfInterleaveGroup drops parity-budget
// packets from one chunk at every position of the interleave group: the chunks
// of a group complete in the same round, so a chunk still waiting for its last
// packet must not be discarded because its neighbours are already restored.
func TestDecoderRecoversLossInEveryChunkOfInterleaveGroup(t *testing.T) {
	const chunks = 4 * recoveryGroup
	for dropCsn := uint64(1); dropCsn <= 2*recoveryGroup; dropCsn++ {
		ctx, cancel := context.WithCancel(context.Background())
		decoder, err := NewDecoder(recoveryDataParts, recoveryTotalParts, 64, 12)
		require.NoError(t, err)
		decoderIn, decoderOut, err := decoder.Decode(ctx)
		require.NoError(t, err)

		sent, got := recoveryRun(t, ctx, decoderIn, decoderOut, chunks, func(p *PacketV2) bool {
			// lose as many packets as there is parity, data packets included
			return p.csn == dropCsn && p.psn < recoveryTotalParts-recoveryDataParts
		})
		cancel()

		require.Len(t, got, chunks, "dropped packets of csn %d", dropCsn)
		for i := range sent {
			require.True(t, bytes.Equal(sent[i], got[i]), "chunk %d differs, dropped csn %d", i+1, dropCsn)
		}
	}
}

// TestDecoderFollowsRestartedEncoder feeds one decoder from two encoder runs in
// a row, like a sender that was restarted while the receiver kept running: the
// second run starts again from csn 1 and must be delivered.
func TestDecoderFollowsRestartedEncoder(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	decoder, err := NewDecoder(recoveryDataParts, recoveryTotalParts, 64, 12)
	require.NoError(t, err)
	decoderIn, decoderOut, err := decoder.Decode(ctx)
	require.NoError(t, err)

	var keepAll = func(*PacketV2) bool { return false }
	for run := range 2 {
		sent, got := recoveryRun(t, ctx, decoderIn, decoderOut, 2*recoveryGroup, keepAll)
		require.Len(t, got, len(sent), "encoder run %d", run+1)
		for i := range sent {
			require.True(t, bytes.Equal(sent[i], got[i]), "run %d chunk %d differs", run+1, i+1)
		}
	}
}

func TestChunkUnmarshalRejectsSizeBeyondParts(t *testing.T) {
	var chunk, _ = NewChunk(2, 1, KindData, []byte("payload"))
	var parts = chunk.Marshal(2)
	// size field at 14...22 of the first part
	for i := 14; i < 22; i++ {
		parts[0][i] = 0xff
	}
	require.False(t, new(Chunk).Unmarshal(parts))
}
