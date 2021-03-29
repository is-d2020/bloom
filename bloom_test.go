package bloom

import (
	"testing"

	"github.com/sohaha/zlsgo"
)

func TestBloom(tt *testing.T) {
	t := zlsgo.NewTest(tt)

	filter := New(1024, 2, true)

	filter.Add([]byte("Hello")).AddString("World")
	filter.AddInt(2021)
	filter.AddIntBatch([]int{2022, 2023})
	filter.AddUInt16(16).AddUInt32(uint32(32)).AddUInt64(uint64(64))
	filter.AddUint16Batch([]uint16{161, 162}).AddUint32Batch([]uint32{321, 322}).AddUin64Batch([]uint64{641, 642})

	for i, b := range map[string]bool{
		"Hello":        filter.Check([]byte("Hello")),
		"World":        filter.CheckString("World"),
		"Hello String": filter.CheckString("Hello"),
		"2021":         filter.CheckInt(2021),
		"2022":         filter.CheckInt(2022),
		"2023":         filter.CheckInt(2023),
		"16":           filter.CheckUInt16(16),
		"32":           filter.CheckUInt32(32),
		"64":           filter.CheckUInt64(64),
		"2024":         !filter.CheckInt(2024),
	} {
		if !b {
			t.Fatal(i)
		}
	}

	t.Log(filter.Cap())
	t.Log(filter.Size())
	t.Equal(uint64(14), filter.Size())
	t.Log(filter.FPR())

	filter.Reset()

	t.Equal(uint64(0), filter.Size())
	t.Log(filter.FPR())
}

func TestMerge(tt *testing.T) {
	t := zlsgo.NewTest(tt)

	filter := New(1024, 2, false)
	filter.Add([]byte("Hello")).AddString("World")

	filter2 := New(1024, 2, true)
	err := filter2.Merge(filter)
	t.EqualNil(err)
	t.EqualTrue(filter2.Check([]byte("Hello")))

	filter3 := New(1, 2, false)
	err = filter3.Merge(filter2)
	t.EqualTrue(err != nil)
	tt.Log(err)
	t.EqualTrue(!filter3.Check([]byte("Hello")))

	filter4 := New(1024, 2, false)
	err = filter4.Merge(filter2)
	t.EqualTrue(err != nil)
	tt.Log(err)
	t.EqualTrue(!filter4.Check([]byte("Hello")))

	filter5 := New(1024, 3, false)
	err = filter5.Merge(filter)
	t.EqualTrue(err != nil)
	tt.Log(err)
	t.EqualTrue(!filter5.Check([]byte("Hello")))
}

func BenchmarkBloom(b *testing.B) {
	filter := New(1024, 3, false)
	filter.AddString("Hello").Add([]byte("Hello"))
	for i := 0; i < b.N; i++ {
		bb := filter.CheckString("Hello")
		if !bb {
			b.Fatal("不匹配")
		}
		bb = filter.CheckString("Hello2")
		if bb {
			b.Fatal("真奇怪匹配")
		}
	}
}
