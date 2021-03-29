package bloom

import (
	"testing"
)

func TestTo(t *testing.T) {
	i1 := uint16ToBytes(uint16(int(1)))
	t.Log(i1)
}
