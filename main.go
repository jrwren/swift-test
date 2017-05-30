package main

import (
	"archive/zip"
	"bytes"
	"compress/flate"
	"flag"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"gopkg.in/errgo.v1"
	"gopkg.in/goose.v2/client"
	"gopkg.in/goose.v2/identity"
	"gopkg.in/goose.v2/swift"
)

var (
	_ = strings.NewReader("")
)

func main() {
	authURL := flag.String("A", "", "auth URL, OS_AUTH_URL is also used.")
	container := flag.String("c", "", "container name")
	oname := flag.String("o", "", "object name")
	n := flag.Int("n", 1, "number of times to do each operation")
	z := flag.String("z", "", "unzip a file by a name from a zip file. alt: colon prefix with its offset and size e.g. 10:20:myfile")
	l := flag.String("l", "", "list zip contents")
	flag.Parse()
	if *oname == "" || *container == "" {
		log.Print("use -o and -c")
	}
	if *authURL == "" {
		*authURL = os.Getenv("OS_AUTH_URL")
	}
	if *authURL == "" {
		*authURL = "http://127.0.0.1:8080/auth/v1.0"
	}
	logger := log.New(os.Stderr, "", log.LstdFlags)
	// don't validate ssl, just testing.
	//c := client.NewNonValidatingClient(&identity.Credentials{
	c := client.NewClient(&identity.Credentials{
		URL:     *authURL,
		User:    "admin:admin",
		Secrets: "admin",
		//		TenantName: "admin", // or UserDomain, or ProjectDomain and docs are useless.
	},
		identity.AuthLegacy,
		logger)
	s := swift.New(c)
	log.Print("reading ", *container, *oname, "with Reader")
	start := time.Now()
	for i := 0; i < *n; i++ {
		req2, m, err := s.GetReadHandle(*container, *oname)
		if err != nil {
			log.Print("failed to get handle to ", *oname, err)
		}
		size, err := strconv.ParseInt(m.Get("Content-Length"), 10, 64)
		if err != nil {
			log.Print("failed to convert size:", err)
		}
		if *l != "" {
			listZipContents(req2, size)
		}
		if *z != "" {
			offset, size := getOffsetAndSize(*z)
			r, err := ZipFileReader(req2, offset, size, true)
			if err != nil {
				log.Print("ZipFileReader error:", err)
			}
			c, err := ioutil.ReadAll(r)
			log.Print(string(c))
		}
		// always 8k blocks from Discard/ReadFrom
		//_, err = ioutil.Discard.(io.ReaderFrom).ReadFrom(req2)
		if err != nil {
			log.Print("failed to read handle for README.md:", err)
		}
	}
	log.Print("done in ", time.Now().Sub(start))
	log.Print("reading one request")
	start = time.Now()
	for i := 0; i < *n; i++ {
		r, m, err := s.GetReader(*container, *oname)
		if err != nil {
			log.Print("failed to get handle to ", *oname, err)
		}
		size, err := strconv.ParseInt(m.Get("Content-Length"), 10, 64)
		if err != nil {
			log.Print("failed to convert size:", err)
		}
		var buf bytes.Buffer
		buf.ReadFrom(r)
		r2 := bytes.NewReader(buf.Bytes())
		if *l != "" {
			listZipContents(r2, size)
		}
		if *z != "" {
			offset, size := getOffsetAndSize(*z)
			r, err := ZipFileReader(r2, offset, size, true)
			if err != nil {
				log.Print("ZipFileReader error:", err)
			}
			c, err := ioutil.ReadAll(r)
			log.Print(string(c))
		}
	}
	log.Print("done in ", time.Now().Sub(start))

	return
}

func getOffsetAndSize(z string) (offset, size int64) {
	zs := strings.Split(z, ":")
	offset, err := strconv.ParseInt(zs[0], 10, 64)
	if err != nil {
		log.Print("could not parse offset:", offset)
	}
	size, err = strconv.ParseInt(zs[1], 10, 64)
	if err != nil {
		log.Print("could not parse size:", size)
	}
	return
}

func listZipContents(r io.ReadSeeker, size int64) {
	zipReader, err := zip.NewReader(&readerAtSeeker{r: r}, size)
	if err != nil {
		log.Print("error listing contnets:", err)
	}
	for _, f := range zipReader.File {
		offset, err := f.DataOffset()
		if err != nil {
			log.Print(f.Name, " could not find offset", f.CompressedSize64)
		}
		log.Print(f.Name, " ", offset, f.CompressedSize64)
	}

}

// zipfilereader adapted from charmstore

// ZipFileReader returns a reader that will read
// content referred to by f within zipr, which should
// refer to the contents of a zip file,
func ZipFileReader(zipr io.ReadSeeker, offset, size int64, compressed bool) (io.Reader, error) {
	if _, err := zipr.Seek(offset, 0); err != nil {
		return nil, errgo.Notef(err, "cannot seek to %d in zip content", offset)
	}
	content := io.LimitReader(zipr, size)
	if !compressed {
		return content, nil
	}
	return flate.NewReader(content), nil
}

// ReaderAtSeeker impl from charmstore

// readerAtSeeker adapts an io.ReadSeeker to an io.ReaderAt.
type readerAtSeeker struct {
	r   io.ReadSeeker
	off int64
}

// ReadAt implemnts SizeReaderAt.ReadAt.
func (r *readerAtSeeker) ReadAt(buf []byte, off int64) (n int, err error) {
	if off != r.off {
		_, err = r.r.Seek(off, 0)
		if err != nil {
			return 0, err
		}
		r.off = off
	}
	n, err = io.ReadFull(r.r, buf)
	r.off += int64(n)
	return n, err
}

// ReaderAtSeeker adapts r so that it can be used as
// a ReaderAt. Note that, contrary to the io.ReaderAt
// contract, it is not OK to use concurrently.
func ReaderAtSeeker(r io.ReadSeeker) io.ReaderAt {
	return &readerAtSeeker{r, 0}
}
