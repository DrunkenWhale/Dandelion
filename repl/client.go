package repl

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
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
