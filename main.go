package main

import (
	"flag"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"time"

	"gopkg.in/goose.v2/client"
	"gopkg.in/goose.v2/identity"
	"gopkg.in/goose.v2/swift"
)

var (
	_ = strings.NewReader("")
)

func main() {
	container := flag.String("c", "", "container name")
	oname := flag.String("o", "", "object name")
	flag.Parse()
	if *oname == "" || *container == "" {
		log.Print("use -o and -c")
	}
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
	log.Print("reading ", *container, *oname, "with Reader")
	start := time.Now()
	req2, _, err := s.GetReadHandle(*container, *oname)
	if err != nil {
		log.Print("failed to get handle to ", *oname, err)
	}
	_, err = ioutil.ReadAll(req2)
	if err != nil {
		log.Print("failed to read handle for README.md:", err)
	}
	log.Print("done in ", time.Now().Sub(start)/time.Millisecond, "ms")
	log.Print("reading one request")
	start = time.Now()
	_, err = s.GetObject(*container, *oname)
	if err != nil {
		log.Print("failed to get handle to ", *oname, err)
	}
	log.Print("done in ", time.Now().Sub(start)/time.Millisecond, "ms")

	return
}
