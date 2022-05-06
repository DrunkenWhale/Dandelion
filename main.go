package main

import (
	lsm2 "Dandelion/lsm"
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
)

func main() {

}

const (
	port = 11451
)

func startClient() {
	conn, err := net.Dial("tcp", "0.0.0.0:11451")
	if err != nil {
		fmt.Printf("dial failed, err:%v\n", err)
		return
	}

	fmt.Println("Conn Established...:")

	reader := bufio.NewReader(os.Stdin)
	for {
		data, err := reader.ReadString('\n')
		if err != nil {
			fmt.Printf("read from console failed, err:%v\n", err)
			break
		}
		data = strings.TrimSpace(data)
		_, err = conn.Write([]byte(data))
		if err != nil {
			fmt.Printf("write failed, err:%v\n", err)
			break
		}
	}
}

func startServer() {
	lsm := lsm2.NewLSM()
	listen, err := net.Listen("tcp", "0.0.0.0:11451")
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
				key, err := strconv.Atoi(opArrays[1])
				if err != nil {
					log.Fatalln(err)
				}
				if opArrays[0] == "put" {
					err := lsm.Put(key, []byte(opArrays[2]))
					if err != nil {
						log.Fatalln(err)
					}
					_, err = conn.Write([]byte("OK"))
					if err != nil {
						log.Fatalln(err)
						return
					}
				} else if opArrays[0] == "get" {
					if bytes, ok := lsm.Get(key); ok {
						_, err := conn.Write(bytes)
						if err != nil {
							log.Fatalln(err)
							return
						}
					} else {
						_, err := conn.Write([]byte("Key Unexist"))
						if err != nil {
							log.Fatalln(err)
							return
						}
					}
				} else if opArrays[0] == "update" {
					err := lsm.Update(key, []byte(opArrays[2]))
					if err != nil {
						log.Fatalln(err)
						return
					}
					_, err = conn.Write([]byte("Update Succeed"))
					if err != nil {
						log.Fatalln(err)
						return
					}
				} else if opArrays[0] == "delete" {
					err := lsm.Delete(key)
					if err != nil {
						log.Fatalln(err)
					}
					_, err = conn.Write([]byte("Delete Succeed"))
					if err != nil {
						log.Fatalln(err)
						return
					}
				} else {
					log.Fatalln("Illegal operation")
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
