package main

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strings"

	"gopkg.in/goose.v2/client"
	"gopkg.in/goose.v2/identity"
	"gopkg.in/goose.v2/swift"
)

var (
	_ = strings.NewReader("")
)

func main() {
	logger := log.New(os.Stderr, "", log.LstdFlags)
	// don't validate ssl, just testing.
	//c := client.NewNonValidatingClient(&identity.Credentials{
	c := client.NewClient(&identity.Credentials{
		//		URL:     "http://10.0.5.171:8080/auth/v1.0",
		URL:     "http://127.0.0.1:8080/auth/v1.0",
		User:    "admin:admin",
		Secrets: "admin",
		//		TenantName: "admin", // or UserDomain, or ProjectDomain and docs are useless.
	},
		identity.AuthLegacy,
		logger)
	s := swift.New(c)
	data := "test1 content"
	err := s.PutReader("t", "test1", bytes.NewReader([]byte(data)), int64(len(data)))
	if err != nil {
		log.Print("failed to put to test1:", err)
	}
	req2, headers, err := s.GetReadHandle("t", "README.md")
	if err != nil {
		log.Print("failed to get handle to README.md:", err)
	}
	log.Print("headers:", headers)
	req2.Seek(500, io.SeekCurrent)
	d, err := ioutil.ReadAll(req2)
	if err != nil {
		log.Print("failed to read handle for README.md:", err)
	}
	fmt.Println("read:", string(d))

	return
}
