package main

import (
	"context"
	"crypto/ecdh"
	"crypto/rand"
	"encoding/hex"
	"flag"
	"fmt"
	"github.com/atotto/clipboard"
	"github.com/go-faster/errors"
	"github.com/gofrs/uuid/v5"
	"github.com/gosuri/uilive"
	"github.com/imakiri/stream/src"
	"github.com/imakiri/stream/src/codec"
	observer2 "github.com/imakiri/stream/src/observer"
	secure2 "github.com/imakiri/stream/src/secure"
	"github.com/pelletier/go-toml"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"time"
)

type Handler struct {
	output *uilive.Writer

	readerOutput chan []byte
	writerOutput chan []byte

	secret []byte
}

func NewHandler(output *uilive.Writer, secret []byte) (*Handler, error) {
	if output == nil {
		return nil, errors.New("output cannot be nil")
	}

	var h = new(Handler)
	h.readerOutput = make(chan []byte, 2)
	h.writerOutput = make(chan []byte, 2)
	h.secret = secret
	h.output = output

	go func() {
		var line []byte
		var err error
		for {
			line = <-h.readerOutput
			_, err = output.Newline().Write(line)
			if err == io.EOF {
				return
			}

			line = <-h.writerOutput
			_, err = output.Newline().Write(line)
			if err == io.EOF {
				return
			}
		}
	}()

	return h, nil
}

func (h *Handler) Reader(server net.Conn) (reader io.ReadCloser, err error) {
	reporter, err := src.NewReporter(h.readerOutput, "in ")
	if err != nil {
		return nil, errors.Wrap(err, "fec.NewReporter")
	}

	reader, err = observer2.NewReader(server, reporter, time.Second)
	if err != nil {
		return nil, errors.Wrap(err, "observer.NewWriter")
	}

	//reader = unit.NewReader(reader, codec.PacketV2Size, true, false)

	if h.secret != nil {
		reader, err = secure2.NewReader(codec.PacketV2Size, h.secret, reader)
		if err != nil {
			return nil, errors.Wrap(err, "secure.NewReader")
		}
	}

	return reader, nil
}

func (h *Handler) Writer(server net.Conn) (writer io.WriteCloser, err error) {
	reporter, err := src.NewReporter(h.writerOutput, "out")
	if err != nil {
		return nil, errors.Wrap(err, "fec.NewReporter")
	}

	writer, err = observer2.NewWriter(server, reporter, time.Second)
	if err != nil {
		return nil, errors.Wrap(err, "observer.NewWriter")
	}

	//writer = unit.NewWriter(writer, codec.PacketV2DataSize, false, true)

	if h.secret != nil {
		writer, err = secure2.NewWriter(codec.PacketV2Size, h.secret, writer)
		if err != nil {
			return nil, errors.Wrap(err, "secure.NewWriter")
		}
	}

	return writer, nil
}

var curve = ecdh.P256()

func mustNewPrivateKey(key string) *ecdh.PrivateKey {
	var prk *ecdh.PrivateKey
	defer func() {
		log.Printf("your public key: %x\n", prk.PublicKey().Bytes())
		clipboard.WriteAll(hex.EncodeToString(prk.PublicKey().Bytes()))
	}()

	var raw, err = hex.DecodeString(key)
	if err == nil {
		prk, err = curve.NewPrivateKey(raw)
		if err == nil {

			return prk
		}
	}
	log.Println("generating new private key")
	prk, err = curve.GenerateKey(rand.Reader)
	if err != nil {
		panic(err)
	}
	return prk
}

func newPublicKey(key string) *ecdh.PublicKey {
	var puk *ecdh.PublicKey
	for {
		var raw, err = hex.DecodeString(key)
		if err == nil {
			puk, err = curve.NewPublicKey(raw)
			if err == nil {
				return puk
			}
		}
		log.Println("enter public key of a second peer")
		fmt.Scan(&key)
	}
}

type Config struct {
	PeerID   uuid.UUID
	Security struct {
		PrivateKey *ecdh.PrivateKey
		PublicKey  *ecdh.PublicKey
	}
	ClientConfig *src.Config
}

func NewConfig(path string) (*Config, error) {
	var file, err = os.OpenFile(path, os.O_RDWR, 0775)
	if err != nil {
		return nil, errors.Wrap(err, "os.OpenFile")
	}

	var cfg = new(struct {
		Peer struct {
			ID           string `toml:"id"`
			PortCaller   uint16 `toml:"port_caller"`
			PortListener uint16 `toml:"port_listener"`
		} `toml:"peer"`
		Fec struct {
			DataParts  uint64 `toml:"data_parts"`
			TotalParts uint64 `toml:"total_parts"`
		} `toml:"fec"`
		Server struct {
			Network string `toml:"network"` // "udp" (default) or "tcp"
			Proxy   string `toml:"proxy"`   // tcp only, e.g. socks5://127.0.0.1:2080
			Port    uint16 `toml:"port"`
			Addr    string `toml:"addr"`
		} `toml:"server"`
		Security struct {
			PrivateKey string `toml:"private_key"`
			PublicKey  string `toml:"public_key"`
		} `toml:"security"`
		Encoder struct {
			DispatcherTimeout uint64 `toml:"dispatcher_timeout"`
			DispatcherSize    uint64 `toml:"dispatcher_size"`
		} `toml:"encoder"`
		Decoder struct {
			AssemblerSize  uint64 `toml:"assembler_size"`
			DispatcherSize uint64 `toml:"dispatcher_size"`
		} `toml:"decoder"`
	})

	err = toml.NewDecoder(file).Decode(cfg)
	if err != nil {
		return nil, errors.Wrap(err, "f.Unmarshal(cfg)")
	}

	var config = new(Config)
	config.ClientConfig = new(src.Config)
	config.ClientConfig.Peer.PortCaller = cfg.Peer.PortCaller
	config.ClientConfig.Peer.PortListener = cfg.Peer.PortListener
	config.ClientConfig.Server.Network = src.ServerNetwork(cfg.Server.Network)
	config.ClientConfig.Server.Proxy = cfg.Server.Proxy
	config.ClientConfig.Server.Port = cfg.Server.Port
	config.ClientConfig.Server.Addr = cfg.Server.Addr
	config.ClientConfig.Fec.TotalParts = cfg.Fec.TotalParts
	config.ClientConfig.Fec.DataParts = cfg.Fec.DataParts
	config.ClientConfig.Decoder.DispatcherSize = cfg.Decoder.DispatcherSize
	config.ClientConfig.Decoder.AssemblerSize = cfg.Decoder.AssemblerSize
	config.ClientConfig.Encoder.DispatcherSize = cfg.Encoder.DispatcherSize
	config.ClientConfig.Encoder.DispatcherTimeout = cfg.Encoder.DispatcherTimeout
	config.PeerID, err = uuid.FromString(cfg.Peer.ID)
	if err != nil {
		return nil, errors.Wrap(err, "uuid.FromString(cfg.PeerID)")
	}

	file.Seek(0, 0)

	config.Security.PrivateKey = mustNewPrivateKey(cfg.Security.PrivateKey)
	cfg.Security.PrivateKey = hex.EncodeToString(config.Security.PrivateKey.Bytes())

	config.Security.PublicKey = newPublicKey(cfg.Security.PublicKey)
	cfg.Security.PublicKey = hex.EncodeToString(config.Security.PublicKey.Bytes())

	return config, toml.NewEncoder(file).Encode(cfg)
}

func main() {
	var cfg = flag.String("cfg", "client.cfg", "path to toml config file")
	flag.Parse()

	var output = uilive.New()
	output.RefreshInterval = time.Second
	output.Start()
	log.SetOutput(output.Bypass())

	var config, err = NewConfig(*cfg)
	if err != nil {
		log.Fatalln(errors.Wrap(err, "NewConfig"))
	}

	client, err := src.NewClient(config.ClientConfig)
	if err != nil {
		log.Fatalln(errors.Wrap(err, "fec.NewClient"))
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)
	defer cancel()

	secret, err := config.Security.PrivateKey.ECDH(config.Security.PublicKey)
	if err != nil {
		log.Fatalln(errors.Wrap(err, "ECDH"))
	}
	log.Printf("your common secret: %x", secret)

	handler, err := NewHandler(output, secret)
	if err != nil {
		log.Fatalln(errors.Wrap(err, "NewHandler"))
	}

	err = client.Run(ctx, config.PeerID, handler)
	if err != nil {
		log.Fatalln(errors.Wrap(err, "fec.NewClient"))
	}

	<-ctx.Done()
	output.Stop()
}
