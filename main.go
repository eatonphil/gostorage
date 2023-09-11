package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math/rand"
	"os"
	"time"
)

type logEntry struct {
	key   []byte
	value []byte
}

// Minimum size to read/write to/from disk.
const CHUNK_SIZE = 4096

// Writes in 4096 byte chunks
func (le *logEntry) write(w io.Writer) int {
	nChunks := (16+len(le.key)+len(le.value))/CHUNK_SIZE + 1
	chunks := make([]byte, nChunks*CHUNK_SIZE)

	// Key first: 8 byte length prefix + actual key
	binary.LittleEndian.PutUint64(chunks, uint64(len(le.key)))
	copy(chunks[8:], le.key)

	// Value next: 8 byte length prefix + actual value
	binary.LittleEndian.PutUint64(chunks[8+len(le.key):], uint64(len(le.key)))
	copy(chunks[8+len(le.key)+8:], le.value)

	// Write chunks to disk
	offset := 0
	for offset < len(chunks) {
		n, err := w.Write(chunks[offset:])
		if err != nil && err != io.ErrShortWrite {
			panic(err)
		}

		offset += n
	}

	return nChunks * CHUNK_SIZE
}

func (le logEntry) readChunk(r io.Reader, chunk *[CHUNK_SIZE]byte) bool {
	var nRead int
	for nRead < CHUNK_SIZE {
		n, err := r.Read(chunk[nRead:])
		if err == io.EOF {
			return true
		}

		if err != nil {
			panic(err)
		}

		nRead += n
	}

	return false
}

// Reads `nBytes` into `into` chunk at a time from `initialOffset`
func (le logEntry) copyUntil(
	r io.Reader,
	into []byte,
	nBytes uint64,
	chunk *[CHUNK_SIZE]byte,
	initialOffset int,
) int {
	bytesRemainingInLastChunk := 0
	if nBytes <= uint64(CHUNK_SIZE-initialOffset) {
		// int(nBytes) conversion here is fine because it's definitely a small size
		copy(into, chunk[initialOffset:initialOffset+int(nBytes)])
		bytesRemainingInLastChunk = CHUNK_SIZE - (initialOffset + int(nBytes))
	} else {
		copy(into, chunk[initialOffset:])
		nBytes -= uint64(CHUNK_SIZE - initialOffset)

		offset := CHUNK_SIZE - initialOffset
		for {
			le.readChunk(r, chunk)
			if nBytes < CHUNK_SIZE {
				copy(into[offset:], chunk[:nBytes])
				// In this case, nBytes will be small enough that this conversion is not wrong
				bytesRemainingInLastChunk = CHUNK_SIZE - int(nBytes)
				break
			} else {
				copy(into[offset:], chunk[:])
				bytesRemainingInLastChunk = 0
			}
			nBytes -= CHUNK_SIZE
			offset += CHUNK_SIZE
		}
	}

	return bytesRemainingInLastChunk
}

func (le *logEntry) read(r io.Reader) bool {
	// Grab the first chunk (to at least get the key size and first N bytes).
	var chunk [CHUNK_SIZE]byte
	eof := le.readChunk(r, &chunk)
	if eof {
		return true
	}
	keySize := binary.LittleEndian.Uint64(chunk[:])
	key := make([]byte, keySize)

	// Grab the key
	bytesRemainingInLastKeyChunk := le.copyUntil(r, key, keySize, &chunk, 8)

	// Grab enough bytes to determine the value size
	var valueSizeBytes [8]byte
	offset := CHUNK_SIZE - bytesRemainingInLastKeyChunk
	copy(valueSizeBytes[:], chunk[offset:])
	if bytesRemainingInLastKeyChunk < 8 {
		le.readChunk(r, &chunk)
		copy(valueSizeBytes[bytesRemainingInLastKeyChunk:], chunk[:8-bytesRemainingInLastKeyChunk])
		offset = 8 - bytesRemainingInLastKeyChunk
	}
	valueSize := binary.LittleEndian.Uint64(valueSizeBytes[:])

	// Grab the value
	value := make([]byte, valueSize)
	le.copyUntil(r, value, valueSize, &chunk, offset+8)

	le.key = key
	le.value = value
	return false
}

type appendOnlyLog struct {
	db *os.File
	cursor uint64
	size int64
}

func (l *appendOnlyLog) init(dir string) {
	var err error
	l.db, err = os.OpenFile(dir+"/data.wal", os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		panic(err)
	}

	stat, err := l.db.Stat()
	if err != nil {
		panic(err)
	}

	l.size = stat.Size()
}

func (l *appendOnlyLog) close() {
	l.db.Close()
}

func (l *appendOnlyLog) insert(key, value []byte) {
	_, err := l.db.Seek(l.size, 0)
	if err != nil {
		panic(err)
	}

	entry := logEntry{key: key, value: value}
	l.size += int64(entry.write(l.db))
	fmt.Println("Index location", l.size)
	err = l.db.Sync()
	if err != nil {
		panic(err)
	}
}

func (l *appendOnlyLog) lookup(key []byte) ([]byte, error) {
	_, err := l.db.Seek(0, 0)
	if err != nil {
		panic(err)
	}

	entry := logEntry{}
	for !entry.read(l.db) {
		if bytes.Equal(entry.key, key) {
			return entry.value, nil
		}
	}

	return nil, fmt.Errorf("Key not found")
}

type sortedEntries struct {
	value logEntry
	left *sortedEntries
	right *sortedEntries
}

type sortedImmutableLog struct {
	db *os.File
	entries sortedEntries
}

func (s *sortedImmutableLog) init(dir string) {
	var err error
	s.db, err = os.OpenFile(dir+"/data.wal", os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
	if err != nil {
		panic(err)
	}
}

func (s sortedImmutableLog) close() {
	s.db.Close()
}

func (s *sortedImmutableLog) insert(key, value []byte) {
	_, err := l.db.Seek(l.size, 0)
	if err != nil {
		panic(err)
	}

	entry := logEntry{key: key, value: value}
	l.size += int64(entry.write(l.db))
	fmt.Println("Index location", l.size)
	err = l.db.Sync()
	if err != nil {
		panic(err)
	}
}

func (l *appendOnlyLog) lookup(key []byte) ([]byte, error) {
	_, err := l.db.Seek(0, 0)
	if err != nil {
		panic(err)
	}

	entry := logEntry{}
	for !entry.read(l.db) {
		if bytes.Equal(entry.key, key) {
			return entry.value, nil
		}
	}

	return nil, fmt.Errorf("Key not found")
}

func randomBytes(n int) []byte {
	fd, err := os.Open("/dev/random")
	if err != nil {
		panic(err)
	}

	out := make([]byte, n)
	allRead := 0
	for allRead < n {
		read, err := fd.Read(out[:])
		if err != nil {
			panic(err)
		}

		allRead += read
	}

	return out
}

func main() {
	l := appendOnlyLog{}
	l.init("data")

	fmt.Println("Generating random data")
	entries := [10][]byte{}
	for i := range entries {
		entries[i] = randomBytes(5000)
		//fmt.Printf("generated: %x .. %x\n", entries[i][:5], entries[i][9995:])
	}
	fmt.Println("Done generating random data")

	fmt.Println("Inserting data")
	for _, key := range entries {
		val := key
		l.insert(key[:], val[:])
	}
	fmt.Println("Done inserting data")

	fmt.Println("Querying data")
	querySamples := 1
	avg := 0 * time.Second
	for i := 0; i < querySamples; i++ {
		rand.Seed(time.Now().UnixNano())
		entry := entries[rand.Intn(len(entries))]
		t1 := time.Now()
		res, err := l.lookup(entry[:])
		avg += time.Now().Sub(t1)
		if err != nil {
			panic(err)
		}
		if !bytes.Equal(res, entry[:]) {
			panic(fmt.Sprintf("Key: %x not equal to value: %x", res, entry))
		}
	}
	fmt.Println(avg / time.Duration(querySamples))
}
