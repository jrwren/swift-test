package main

import (
	"archive/zip"
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
	"gopkg.in/mgo.v2"

	"github.com/juju/blobstore"
)

var (
	_ = strings.NewReader("")
)

func main() {
	authURL := flag.String("A", "", "mongourl")
	container := flag.String("c", "", "prefix name")
	oname := flag.String("o", "", "object name")
	n := flag.Int("n", 1, "number of times to do each operation")
	z := flag.String("z", "", "unzip a file by a name from a zip file. alt: colon prefix with its offset and size e.g. 10:20:myfile")
	l := flag.String("l", "", "list zip contents")
	p := flag.String("p", "", "put file")
	flag.Parse()
	if *authURL == "" {
		*authURL = "127.0.0.1"
	}
	session, err := mgo.Dial(*authURL)
	if err != nil {
		log.Print("could not dial ", *authURL, ":", err)
	}
	db := session.DB("test")
	rs := blobstore.NewGridFS(db.Name, *container, db.Session)
	s := blobstore.NewManagedStorage(db, rs)
	if *p != "" {
		f, err := os.Open(*p)
		if err != nil {
			log.Print("error opening ", *p, ":", err)
		}
		st, err := f.Stat()
		if err != nil {
			log.Print("error stating ", *p, ":", err)
		}
		s.PutForEnvironment("", *p, f, st.Size())
		return
	}
	if *oname == "" || *container == "" {
		log.Print("use -o and -c")
		return
	}
	log.Print("reading ", *container, *oname, "with Reader")
	start := time.Now()
	for i := 0; i < *n; i++ {
		r, length, err := s.GetForEnvironment("", *oname)
		if err != nil {
			log.Print("failed to get handle to ", *oname, err)
		}
		req2 := r.(io.ReadSeeker)
		if *l != "" {
			listZipContents(req2, length)
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
