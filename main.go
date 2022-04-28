package main

import (
	"fmt"
	"os"
)

func main() {
	a, _ := os.ReadDir(".")
	for _, e := range a {
		fmt.Println(e.Name())
	}
}
