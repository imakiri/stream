// Command server is the stream source: it takes the mpegts stream from OBS
// over local UDP and sends it with FEC to every client through the proxy.
//
// On the first start it makes a CA and its own certificate in the key
// directory (by default next to the binary). A new client prints its public
// key; sign it with
//
//	server -sign <base64 public key> [-days 365]
//
// and send the printed enrollment back to the client.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/atotto/clipboard"
	"github.com/imakiri/stream/src"
	"github.com/imakiri/stream/src/liveoutput"
)

func main() {
	var cfg = flag.String("cfg", "server.cfg", "path to toml config file, see server.example.cfg")
	var keys = flag.String("keys", "", "key directory; the directory of the binary if empty")
	var sign = flag.String("sign", "", "sign this base64 client public key, print the enrollment and exit")
	var days = flag.Int("days", 365, "with -sign: days the client certificate is valid")
	flag.Parse()

	var keyDir = *keys
	if keyDir == "" {
		var err error
		if keyDir, err = src.DefaultKeyDir(); err != nil {
			log.Fatalln(err)
		}
	}
	identity, err := src.LoadOrCreateServerIdentity(keyDir)
	if err != nil {
		log.Fatalln(err)
	}

	if *sign != "" {
		enrollment, certificate, err := identity.EnrollClient(*sign, time.Duration(*days)*24*time.Hour)
		if err != nil {
			log.Fatalln(err)
		}
		fmt.Printf("client %s, valid until %s; send this enrollment back to the client:\n\n%s\n\n",
			certificate.ID, certificate.NotAfter.Format(time.DateOnly), enrollment)
		if err := clipboard.WriteAll(enrollment); err != nil {
			fmt.Printf("(could not copy it to the clipboard: %s)\n", err)
		} else {
			fmt.Println("(copied to the clipboard)")
		}
		return
	}

	var output = liveoutput.New(os.Stdout)
	log.SetOutput(output.Log())

	config, err := src.LoadServerConfig(*cfg)
	if err != nil {
		log.Fatalln(err)
	}
	config.Credentials = identity.Credentials
	config.Verifier = identity.Verifier()
	config.OutputReporter = src.NewReporter(output, "out")
	log.Printf("server %s", config.Credentials.Certificate.ID)

	server, err := src.NewServer(config)
	if err != nil {
		log.Fatalln(err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	if err := server.Run(ctx); err != nil {
		log.Fatalln(err)
	}
}
