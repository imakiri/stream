// Command proxy relays the stream between the server and its clients.
package main

import (
	"flag"
	"log"
	"os"
	"os/signal"

	"github.com/imakiri/stream/src/liveoutput"
	"github.com/imakiri/stream/src/proxy"
)

func main() {
	var addr = flag.String("addr", "127.0.0.1:25565", "address to listen at, over both udp and tcp")
	flag.Parse()

	var output = liveoutput.New(os.Stdout)
	log.SetOutput(output.Log())

	var p = proxy.New(*addr, output)
	if err := p.Serve(); err != nil {
		log.Fatalln(err)
	}

	var stop = make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt)
	<-stop
	p.Close()
}
