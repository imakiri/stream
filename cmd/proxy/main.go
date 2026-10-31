// Command proxy relays the stream between the server and its clients.
package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/gosuri/uilive"
	"github.com/imakiri/stream/src/proxy"
)

func main() {
	var addr = flag.String("addr", "127.0.0.1:25565", "address to listen at, over both udp and tcp")
	flag.Parse()

	var output = uilive.New()
	output.Start()
	output.RefreshInterval = time.Second
	log.SetOutput(output.Bypass())

	var p = proxy.New(*addr, output)
	if err := p.Serve(); err != nil {
		log.Fatalln(err)
	}

	var stop = make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt)
	<-stop
	p.Close()
	output.Stop()
}
