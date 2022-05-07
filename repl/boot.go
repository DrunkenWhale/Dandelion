package repl

import "log"

func StartRepl(mode string) {
	if mode == "server" {
		startServer()
	} else if mode == "client" {
		startClient()
	} else {
		log.Fatalln("Unknown Boot Argument")
	}
}
