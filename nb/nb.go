package main

import (
	"fmt"
	"github.com/sangstar/FastNB/loader"
)

func main() {
	Probs, err := loader.Load()
	if err != nil {
		panic(err)
	}
	fmt.Println(Probs)
}
