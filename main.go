package main

import (
	"flag"
	"fmt"
	"github.com/bradfitz/gomemcache/memcache"
)

// MemcacheStorage представляет собой структуру для хранения клиента Memcache и других данных, если необходимо.
type MemcacheStorage struct {
	Client *memcache.Client
	// Дополнительные поля, если нужно
}

func NewMemcacheStorage(addr string) *MemcacheStorage {
	// Создание клиента, подключающегося к Memcache серверу по указанному адресу
	mc := memcache.New(addr)
	return &MemcacheStorage{Client: mc}
}

func btos(c []byte) string {
	//function for processing bytestring
	n := 0
	for _, b := range c {
		if b == 0 {
			continue
		}
		c[n] = b
		n++
	}
	return string(c[:n])
}

func main() {
	// Определение флага для адреса подключения к Memcache
	addr := flag.String("memcache", "127.0.0.1:33014", "address of the memcache server")
	flag.Parse()

	// Создание экземпляра хранилища Memcache с указанным адресом
	memcacheStorage := NewMemcacheStorage(*addr)

	// Пример добавления данных в кеш
	err := memcacheStorage.Client.Set(&memcache.Item{Key: "foo", Value: []byte("bar")})
	if err != nil {
		fmt.Println("SET Error setting value in memcache:", err)
		return
	}
	fmt.Println("Value 'foo' successfully added to memcache")

	// Пример получения данных из кеша
	item, err := memcacheStorage.Client.Get("foo")
	fmt.Printf("Value successfully got '%v', memcache '%v'\n", item.Key, string(item.Value))
	if err != nil {
		fmt.Println("GET Error getting value from memcache:", err)
	}
}
