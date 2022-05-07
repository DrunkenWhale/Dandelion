package main

import (
	lsm2 "Dandelion/lsm"
	"bufio"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
)

func main() {
	flag.Parse()
	mode := flag.Arg(0)
	if mode == "server" {
		startServer()
	} else if mode == "client" {
		startClient()
	} else {
		log.Fatalln("Illegal Argument")
	}
}

const (
	port = 11451
)

func startClient() {
	conn, err := net.Dial("tcp", "0.0.0.0:"+strconv.Itoa(port))
	buf := bufio.NewReader(conn)
	if err != nil {
		fmt.Printf("dial failed, err:%v\n", err)
		return
	}

	fmt.Println("Conn Established...:")

	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Print("client> ")
		data, err := reader.ReadString('\n')
		if err != nil {
			fmt.Printf("read from console failed, err:%v\n", err)
			break
		}
		_, err = conn.Write([]byte(data))
		if err != nil {
			fmt.Printf("write failed, err:%v\n", err)
			break
		}
		readString, err := buf.ReadString(3)
		if err != nil {
			log.Fatalln(readString[:len(readString)-1])
			return
		}
		log.Println(readString[:len(readString)-1])
	}
}

func startServer() {
	lsm := lsm2.NewLSM()
	listen, err := net.Listen("tcp", "0.0.0.0:"+strconv.Itoa(port))
	if err != nil {
		log.Fatalln(err)
		return
	}
	fmt.Println("Listen Start")
	for {
		conn, err := listen.Accept()
		if err != nil {
			log.Fatalln(err)
		}
		buf := bufio.NewReader(conn)
		go func() {
			for {
				str, err := buf.ReadString('\n')
				if err != nil {
					fmt.Printf("read from conn failed, err:%v", err)
					break
				}
				order := str
				opArrays := strings.Split(order, " ")
				if len(opArrays) < 2 {
					_, err := conn.Write(append([]byte("Illegal Argument"), 3))
					if err != nil {
						log.Fatalln(err)
						return
					}
					continue
				}
				key, err := strconv.Atoi(strings.TrimSpace(opArrays[1]))
				if err != nil {
					log.Fatalln(err)
				}
				if opArrays[0] == "put" {
					if len(opArrays) < 3 {
						_, err := conn.Write(append([]byte("Illegal Argument"), 3))
						if err != nil {
							log.Fatalln(err)
							return
						}
						continue
					}
					err := lsm.Put(key, []byte(opArrays[2]))
					if err != nil {
						log.Fatalln(err)
					}
					_, err = conn.Write(append([]byte("OK"), 3))
					if err != nil {
						log.Fatalln(err)
						return
					}
				} else if opArrays[0] == "get" {
					if bytes, ok := lsm.Get(key); ok {
						_, err := conn.Write(append(bytes, 3))
						if err != nil {
							log.Fatalln(err)
							return
						}
					} else {
						_, err := conn.Write(append([]byte("Key Unexist"), 3))
						if err != nil {
							log.Fatalln(err)
							return
						}
					}
				} else if opArrays[0] == "update" {
					if len(opArrays) < 3 {
						_, err := conn.Write(append([]byte("Illegal Argument"), 3))
						if err != nil {
							log.Fatalln(err)
							return
						}
						continue
					}
					err := lsm.Update(key, []byte(opArrays[2]))
					if err != nil {
						log.Fatalln(err)
						return
					}
					_, err = conn.Write(append([]byte("Update Succeed"), 3))
					if err != nil {
						log.Fatalln(err)
						return
					}
				} else if opArrays[0] == "delete" {
					err := lsm.Delete(key)
					if err != nil {
						log.Fatalln(err)
					}
					_, err = conn.Write(append([]byte("Delete Succeed"), 3))
					if err != nil {
						log.Fatalln(err)
						return
					}
				} else {
					_, err := conn.Write(append([]byte("Unknown Operation"), 3))
					if err != nil {
						log.Fatalln(err)
						return
					}
				}
			}
			err := conn.Close()
			if err != nil {
				log.Fatalln(err)
				return
			}
		}()
	}
}
