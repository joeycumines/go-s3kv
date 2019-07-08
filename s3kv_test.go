package s3kv

import (
	"crypto/md5"
	"fmt"
	"math/rand"
	"testing"
	"unsafe"
)

func TestReduceBytes(t *testing.T) {
	testCases := []struct {
		Input  []byte
		Output uint32
	}{
		{
			Input:  []byte{},
			Output: 0,
		},
		{
			Input:  []byte{0},
			Output: 0,
		},
		{
			Input:  []byte{1},
			Output: 1,
		},
		{
			Input:  []byte{105},
			Output: 105,
		},
		{
			Input:  []byte{255},
			Output: 255,
		},
		{
			Input:  []byte{255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255},
			Output: 4294967295,
		},
	}

	testUint32 := func(v uint32) {
		var b [4]byte
		b = *(*[4]byte)(unsafe.Pointer(&v))
		b[0], b[1], b[2], b[3] = b[3], b[2], b[1], b[0]
		testCases = append(
			testCases, struct {
				Input  []byte
				Output uint32
			}{
				Input:  b[:],
				Output: v,
			},
		)
	}

	testUint32(912395)
	testUint32(883848199)
	testUint32(4294967295)

	for i := uint64(0); i < 99999; i++ {
		v := 4294967295 + i*13
		var b [8]byte
		b = *(*[8]byte)(unsafe.Pointer(&v))
		b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7] = b[7], b[6], b[5], b[4], b[3], b[2], b[1], b[0]
		testCases = append(
			testCases, struct {
				Input  []byte
				Output uint32
			}{
				Input:  b[:],
				Output: uint32(v - 4294967295 - 1),
			},
		)
	}

	for i, testCase := range testCases {
		name := fmt.Sprintf("TestReduceBytes_#%d", i+1)

		output := ReduceBytes(testCase.Input)

		if output != testCase.Output {
			t.Error(name, "output", output, "!= expected", testCase.Output)
		}
	}
}

func TestHash(t *testing.T) {
	testCases := []struct {
		Segments []Segment
		Opts     []HashOption
		Output   string
	}{
		{
			Segments: []Segment{},
			Output:   ``,
		},
		{
			Segments: []Segment{
				{
					Hash: false,
					Key:  []byte(`some`),
				},
				{
					Hash: false,
					Key:  []byte(`key`),
				},
			},
			Output: `some/key`,
		},
		{
			Segments: []Segment{
				{
					Hash: true,
					Key:  []byte(`value`),
				},
				{
					Hash: false,
					Key:  []byte(`value`),
				},
			},
			Output: `804/value`,
		},
		{
			Segments: []Segment{
				{
					Hash: true,
					Key: []byte(`asd321rSaDSASD!@EDSAF FFG ASDSAD(SAUDSAHDSD

asd
sad 0oi92ieijdsaKSDAKJDS HUSADANJD`),
				},
			},
			Output: `0b2`,
		},
		{
			Segments: []Segment{
				{
					Hash: true,
					Key:  []byte(`this/is/some/path`),
				},
				{
					Hash: false,
					Key:  []byte(`this/is/some/path`),
				},
				{
					Hash: true,
					Key:  []byte(`with/two/hashes`),
				},
				{
					Hash: false,
					Key:  []byte(`with/two/hashes`),
				},
			},
			Output: `8db/this/is/some/path/268/with/two/hashes`,
		},
	}

	for i, testCase := range testCases {
		name := fmt.Sprintf("TestHash_#%d", i+1)

		output := string(Hash(testCase.Segments, testCase.Opts...))

		if output != testCase.Output {
			t.Error(name, "output", output, "!= expected", testCase.Output)
		}
	}
}

func TestPadHex(t *testing.T) {
	testCases := []struct {
		Value  uint32
		Max    uint32
		Output string
	}{
		{
			Value:  0,
			Max:    0,
			Output: `0`,
		},
		{
			Value:  0,
			Max:    15,
			Output: `0`,
		},
		{
			Value:  1,
			Max:    15,
			Output: `1`,
		},
		{
			Value:  9,
			Max:    15,
			Output: `9`,
		},
		{
			Value:  10,
			Max:    15,
			Output: `a`,
		},
		{
			Value:  15,
			Max:    15,
			Output: `f`,
		},
		{
			Value:  16,
			Max:    15,
			Output: `0`,
		},
		{
			Value:  26,
			Max:    15,
			Output: `a`,
		},
		{
			Value:  15,
			Max:    16,
			Output: `0f`,
		},
		{
			Value:  15,
			Max:    4095,
			Output: `00f`,
		},
		{
			Value:  4095,
			Max:    4095,
			Output: `fff`,
		},
		{
			Value:  4096,
			Max:    4095,
			Output: `000`,
		},
		{
			Value:  10,
			Max:    4096,
			Output: `000a`,
		},
		{
			Value:  65535,
			Max:    65535,
			Output: `ffff`,
		},
		{
			Value:  65536,
			Max:    65535,
			Output: `0000`,
		},
		{
			Value:  65535,
			Max:    65536,
			Output: `0ffff`,
		},
		{
			Value:  1048575,
			Max:    1048575,
			Output: `fffff`,
		},
		{
			Value:  1048575,
			Max:    1048576,
			Output: `0fffff`,
		},
		{
			Value:  4294967295,
			Max:    4294967295,
			Output: `ffffffff`,
		},
	}

	for i, testCase := range testCases {
		name := fmt.Sprintf("TestPadHex_#%d", i+1)

		output := PaddedHex(testCase.Value, testCase.Max)

		if output != testCase.Output {
			t.Error(name, "output", output, "!= expected", testCase.Output)
		}
	}
}

func TestHash_opt(t *testing.T) {
	var oneChar HashOption = func(config *HashConfig) {
		config.Max = 15
	}
	if output := string(Hash([]Segment{{Key: []byte(``), Hash: true}}, oneChar)); output != `e` {
		t.Error("unexpected output", output)
	}
	if output := string(Hash([]Segment{{Key: []byte(`0`), Hash: true}}, oneChar)); output != `a` {
		t.Error("unexpected output", output)
	}
	if output := string(Hash([]Segment{{Key: []byte(`asdadsu sad(S(@@#`), Hash: true}}, oneChar)); output != `f` {
		t.Error("unexpected output", output)
	}
	if output := string(Hash([]Segment{{Key: []byte(`Z`), Hash: true}}, nil, oneChar, nil, nil, oneChar)); output != `5` {
		t.Error("unexpected output", output)
	}
	if output := string(Hash([]Segment{{Key: []byte(`z`), Hash: true}}, oneChar, nil, )); output != `7` {
		t.Error("unexpected output", output)
	}
	if output := string(Hash([]Segment{{Key: []byte{0}, Hash: true}}, nil, oneChar)); output != `1` {
		t.Error("unexpected output", output)
	}
	if output := string(Hash([]Segment{{Key: []byte{5}, Hash: true}}, oneChar)); output != `9` {
		t.Error("unexpected output", output)
	}
	// normal behavior without the option
	if output := string(Hash([]Segment{{Key: []byte{0}, Hash: true}})); output != `f71` {
		t.Error("unexpected output", output)
	}
}

var hashed interface{}

func benchmarkHash(b *testing.B, fn func(b []byte) interface{}, l int) {
	for x := 0; x < b.N; x++ {
		b.StopTimer()
		v := make([]byte, l)
		_, _ = rand.Read(v)
		b.StartTimer()
		hashed = fn(v)
	}
}

func BenchmarkReduceBytes_actual(b *testing.B) {
	benchmarkHash(
		b,
		func(b []byte) interface{} {
			return ReduceBytes(b)
		},
		1e6,
	)
	b.Log(hashed)
}

func BenchmarkReduceBytes_md5(b *testing.B) {
	benchmarkHash(
		b,
		func(b []byte) interface{} {
			return md5.Sum(b)
		},
		1e6,
	)
	b.Log(hashed)
}
