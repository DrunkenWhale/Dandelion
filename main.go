package main

import (
	"flag"
	"github.com/Pigeon377/Dandelion/repl"
)

func main() {
	flag.Parse()
	mode := flag.Arg(0)
	repl.StartRepl(mode)
}
