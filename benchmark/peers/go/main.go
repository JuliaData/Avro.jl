// hamba/avro typed-struct reader and writer benchmark.
//
// usage: peerbench read <shape> <file> <iters>
//
//	peerbench write <shape> <infile> <outfile> <codec> <iters>
//
// Prints per iteration `iter <k> <s> <rows> <isum> <nstr> <nbytes> key=value...`.
package main

import (
	"bufio"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"time"

	"github.com/hamba/avro/v2/ocf"
)

const m61 = int64(1)<<61 - 1

type Numeric struct {
	A    int64   `avro:"a"`
	B    int64   `avro:"b"`
	C    int64   `avro:"c"`
	D    int64   `avro:"d"`
	X    float64 `avro:"x"`
	Y    float64 `avro:"y"`
	N    int32   `avro:"n"`
	Flag bool    `avro:"flag"`
}

type Strings struct {
	S0 string `avro:"s0"`
	S1 string `avro:"s1"`
	S2 string `avro:"s2"`
	S3 string `avro:"s3"`
	S4 string `avro:"s4"`
	S5 string `avro:"s5"`
	S6 string `avro:"s6"`
	S7 string `avro:"s7"`
	S8 string `avro:"s8"`
	S9 string `avro:"s9"`
}

type Event struct {
	ID     int64     `avro:"id"`
	Ts     time.Time `avro:"ts"`
	Name   string    `avro:"name"`
	User   *string   `avro:"user"`
	Value  float64   `avro:"value"`
	Count  int32     `avro:"count"`
	Kind   string    `avro:"kind"`
	Parent *int64    `avro:"parent"`
	Ok     bool      `avro:"ok"`
	Tag    []byte    `avro:"tag"`
}

type Point struct {
	Label string  `avro:"label"`
	W     float64 `avro:"w"`
}

type Nested struct {
	ID    int64            `avro:"id"`
	Items []int64          `avro:"items"`
	Attrs map[string]int64 `avro:"attrs"`
	Point Point            `avro:"point"`
}

var kinds = map[string]int64{"CLICK": 0, "VIEW": 1, "PURCHASE": 2, "SIGNUP": 3, "LOGOUT": 4}

type ck struct{ rows, isum, nstr, nbytes int64 }

func (c *ck) str(s string)  { c.nstr++; c.nbytes += int64(len(s)) }
func (c *ck) dbl(x float64) { c.isum += int64(math.Float64bits(x)) }
func b2i(b bool) int64 {
	if b {
		return 1
	}
	return 0
}

func ckNumeric(r *Numeric, c *ck) {
	c.isum += r.A + r.B + r.C + r.D + int64(r.N) + b2i(r.Flag)
	c.dbl(r.X)
	c.dbl(r.Y)
}

func ckStrings(r *Strings, c *ck) {
	for _, s := range [...]string{r.S0, r.S1, r.S2, r.S3, r.S4, r.S5, r.S6, r.S7, r.S8, r.S9} {
		c.str(s)
	}
}

func ckEvent(r *Event, c *ck) {
	c.isum += r.ID + r.Ts.UnixMicro() + int64(r.Count) + kinds[r.Kind] + b2i(r.Ok)
	c.dbl(r.Value)
	if r.Parent != nil {
		c.isum += *r.Parent
	}
	c.str(r.Name)
	if r.User != nil {
		c.str(*r.User)
	}
	c.nstr++
	c.nbytes += int64(len(r.Tag))
}

func ckNested(r *Nested, c *ck) {
	c.isum += r.ID
	for _, x := range r.Items {
		c.isum += x
	}
	for k, v := range r.Attrs {
		c.str(k)
		c.isum += v
	}
	c.str(r.Point.Label)
	c.dbl(r.Point.W)
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}

func decoder(path string) (*ocf.Decoder, *os.File) {
	f, err := os.Open(path)
	must(err)
	d, err := ocf.NewDecoder(bufio.NewReaderSize(f, 1<<16))
	must(err)
	return d, f
}

// readAll decodes every row into one value, zeroed first because Decode merges into a non-nil map.
func readAll[T any](path string, f func(*T, *ck)) ck {
	var c ck
	d, file := decoder(path)
	defer file.Close()
	var r, zero T
	for d.HasNext() {
		r = zero
		must(d.Decode(&r))
		c.rows++
		f(&r, &c)
	}
	must(d.Error())
	return c
}

func load[T any](path string) []T {
	d, file := decoder(path)
	defer file.Close()
	var rows []T
	for d.HasNext() {
		var r T
		must(d.Decode(&r))
		rows = append(rows, r)
	}
	must(d.Error())
	return rows
}

func writeAll[T any](rows []T, schema, out string, codec ocf.CodecName) int {
	f, err := os.Create(out)
	must(err)
	w := bufio.NewWriterSize(f, 1<<16)
	enc, err := ocf.NewEncoder(schema, w, ocf.WithCodec(codec), ocf.WithBlockLength(math.MaxInt32), ocf.WithBlockSize(64000))
	must(err)
	for i := range rows {
		must(enc.Encode(&rows[i]))
	}
	must(enc.Close())
	must(w.Flush())
	must(f.Close())
	return len(rows)
}

func memstats() (uint64, uint64) {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.PauseTotalNs, m.TotalAlloc
}

func report(k int, dt time.Duration, c ck, p0, a0 uint64, extra string) {
	p1, a1 := memstats()
	fmt.Printf("iter %d %.6f %d %d %d %d gc=%.6f alloc=%d%s\n", k, dt.Seconds(), c.rows, c.isum&m61, c.nstr, c.nbytes,
		float64(p1-p0)/1e9, a1-a0, extra)
}

func main() {
	a := os.Args
	switch a[1] {
	case "read":
		shape, path := a[2], a[3]
		iters, _ := strconv.Atoi(a[4])
		for k := 1; k <= iters; k++ {
			p0, a0 := memstats()
			t0 := time.Now()
			var c ck
			switch shape {
			case "numeric":
				c = readAll(path, ckNumeric)
			case "strings":
				c = readAll(path, ckStrings)
			case "mixed":
				c = readAll(path, ckEvent)
			case "nested":
				c = readAll(path, ckNested)
			}
			report(k, time.Since(t0), c, p0, a0, "")
		}
	case "write":
		shape, in, out, codec := a[2], a[3], a[4], ocf.CodecName(a[5])
		iters, _ := strconv.Atoi(a[6])
		exe, _ := os.Executable()
		schema, err := os.ReadFile(filepath.Join(filepath.Dir(exe), "..", "schemas", shape+".avsc"))
		must(err)
		var run func() int
		switch shape {
		case "numeric":
			rows := load[Numeric](in)
			run = func() int { return writeAll(rows, string(schema), out, codec) }
		case "strings":
			rows := load[Strings](in)
			run = func() int { return writeAll(rows, string(schema), out, codec) }
		case "mixed":
			rows := load[Event](in)
			run = func() int { return writeAll(rows, string(schema), out, codec) }
		case "nested":
			rows := load[Nested](in)
			run = func() int { return writeAll(rows, string(schema), out, codec) }
		}
		runtime.GC()
		for k := 1; k <= iters; k++ {
			p0, a0 := memstats()
			t0 := time.Now()
			n := run()
			dt := time.Since(t0)
			st, err := os.Stat(out)
			must(err)
			report(k, dt, ck{rows: int64(n)}, p0, a0, fmt.Sprintf(" out=%d", st.Size()))
		}
	}
}
