package main

import (
	"Dandelion/repl"
	"flag"
)

func main() {
	flag.Parse()
	mode := flag.Arg(0)
	repl.StartRepl(mode)
}
