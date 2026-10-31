// Command client is a viewer: it gets the stream from the server through the
// proxy, restores it with FEC and sends it to a local player over UDP, e.g.
//
//	ffplay -f mpegts -fflags nobuffer -flags low_delay -framedrop "udp://127.0.0.1:25566?overrun_nonfatal=1"
//
// On the first start it makes its key in the key directory (by default next
// to the binary), prints the public key and copies it to the clipboard. Send
// it to the server owner, who signs it with server -sign, and paste the
// enrollment you get back.
package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/gosuri/uilive"
	"github.com/imakiri/stream/src"
)

func main() {
	var cfg = flag.String("cfg", "client.cfg", "path to toml config file, see client.example.cfg")
	var keys = flag.String("keys", "", "key directory; the directory of the binary if empty")
	flag.Parse()

	var keyDir = *keys
	if keyDir == "" {
		var err error
		if keyDir, err = src.DefaultKeyDir(); err != nil {
			log.Fatalln(err)
		}
	}
	// before the live output starts: it may ask for the enrollment
	identity, err := src.LoadOrEnrollClientIdentity(keyDir, os.Stdin, os.Stdout)
	if err != nil {
		log.Fatalln(err)
	}

	var output = uilive.New()
	output.RefreshInterval = time.Second
	output.Start()
	defer output.Stop()
	log.SetOutput(output.Bypass())

	config, err := src.LoadClientConfig(*cfg)
	if err != nil {
		log.Fatalln(err)
	}
	config.Credentials = identity.Credentials
	config.Verifier = identity.Verifier()
	config.ServerID = identity.Enrollment.ServerID
	config.InputReporter = src.NewReporter(output, "in ")
	log.Printf("client %s, certificate valid until %s", config.Credentials.Certificate.ID, config.Credentials.Certificate.NotAfter.Format(time.DateOnly))

	client, err := src.NewClient(config)
	if err != nil {
		log.Fatalln(err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	if err := client.Run(ctx); err != nil {
		log.Fatalln(err)
	}
}
