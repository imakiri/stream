package codec

import (
	"bytes"
	"context"
	"crypto/rand"
	"github.com/stretchr/testify/require"
	"os"
	"os/signal"
	"sync"
	"testing"
	"time"
)

func TestCodec(t *testing.T) {
	const dataParts = 4
	const totalParts = 8
	var err error
	var ctx, cancel = signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)
	defer cancel()

	var expecting = bytes.NewBuffer(nil)
	var got = bytes.NewBuffer(nil)

	var encoder *Encoder
	var decoder *Decoder

	encoder, err = NewEncoder(20*time.Millisecond, 32, dataParts, totalParts)
	require.NoError(t, err)

	decoder, err = NewDecoder(dataParts, totalParts, 128, 16)
	require.NoError(t, err)

	encoderIn, encoderOut, err := encoder.Encode(ctx)
	require.NoError(t, err)
	require.NotNil(t, encoderIn)
	require.NotNil(t, encoderOut)

	decoderIn, decoderOut, err := decoder.Decode(ctx)
	require.NoError(t, err)
	require.NotNil(t, decoderIn)
	require.NotNil(t, decoderOut)

	var wg = new(sync.WaitGroup)
	const chunks = 32
	wg.Add(1)
	go func() {
		var size = encoder.IncomingSize() - 3
		for i := 0; i < chunks; i++ {
			var buf = make([]byte, size)

			n, err := rand.Read(buf)
			require.NoError(t, err)
			require.EqualValues(t, size, n)

			n, err = expecting.Write(buf)
			require.NoError(t, err)
			require.EqualValues(t, size, n)

			encoderIn <- buf
		}

		//var data = make([]byte, encoder.IncomingSize())
		//for i := range data {
		//	data[i] = 1
		//}
		//encoderIn <- data
		//expecting.Write(data)
		//
		//data = make([]byte, encoder.IncomingSize())
		//for i := range data {
		//	data[i] = 2
		//}
		//encoderIn <- data
		//expecting.Write(data)
	}()

	go func() {
		var data []byte
		var packet *PacketV2
		var ok bool
		for {
			select {
			case <-ctx.Done():
				return
			case data = <-encoderOut:
				// no packet loss for now
				packet = new(PacketV2)
				ok = packet.Unmarshal(data)
				require.True(t, ok)
				//t.Log(packet)
				select {
				case decoderIn <- data:
					continue
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	go func() {
		for i := 0; i < chunks; i++ {
			select {
			case <-ctx.Done():
				return
			case chunk := <-decoderOut:
				var n, err = got.Write(chunk.Data)
				require.NoError(t, err)
				require.EqualValues(t, len(chunk.Data), n)
				// the encoder stamps every chunk with when its data came in
				var delay = InputDelay(chunk.InputUnixMilli, time.Now())
				require.GreaterOrEqual(t, delay, time.Duration(0))
				require.Less(t, delay, 5*time.Second)
			}
		}
		wg.Done()
	}()

	wg.Wait()
	t.Log(got.Bytes()[20])
	t.Log(expecting.Bytes()[20])

	require.EqualValues(t, expecting.Len(), got.Len())
	require.EqualValues(t, expecting.Bytes(), got.Bytes())
}
