# Dandelion

an easy, delicate kv storage engine based on lsm

## use with golang

```go

package main

import (
	"Dandelion/lsm"
	"Dandelion/sstable"
	"log"
	"math/rand"
	"strconv"
)

func main() {
	l := lsm.NewLSM()
	for _, i := range rand.Perm(1145147) {
		err := l.Put(i, []byte(strconv.Itoa(i)))
		if err != nil {
			log.Fatalln(err)
		}
	}
	err := sstable.MergeLevel0File()
	if err != nil {
		log.Fatalln(err)
	}
	values, ok := l.Get(114)
	log.Println(string(values), ok)
	// output 114, true
	err = l.Update(114, []byte("114514"))
	err = l.Delete(114)
	values, ok = l.Get(114)
	err = l.Flush()
	if err != nil {
		log.Fatalln(err)
		return 
	}
	//output nil, false
	log.Println(string(values), ok)
}
```

## using repl

start server

```shell

 .\dandelion server
 
```

start client

```shell

.\dandelion client

```

and you will see this

```shell

client>             

```

this repl is humble

only support some sentence

like this

```shell

client> put 114514 1919810
2022/05/07 10:11:35 OK
client> put 114514
2022/05/07 10:11:38 OK
client> get 7
2022/05/07 10:11:43 Key Unexist
client> put 7 sdasdsgdsdgsdf
2022/05/07 10:11:48 OK
client> get 7
2022/05/07 10:11:52 sdasdsgdsdgsdf

client> update 114514 SoundOfSilence  
2022/05/07 10:12:26 Update Succeed
client> get 114514                
2022/05/07 10:12:29 SoundOfSilence
                                  
client> delete 114514             
2022/05/07 10:12:32 Delete Succeed
client> get 114514                
2022/05/07 10:12:34 Key Unexist


```

