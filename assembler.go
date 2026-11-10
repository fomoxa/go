package fomoxa

import "bytes"

type assembly int

const (
	assemblyPending assembly = iota
	assemblyComplete
	assemblyBroken
)

type assembler struct {
	content    []byte
	received   []bool
	count      int
	arrived    int
	lastLength int
}

func (a *assembler) pending() bool { return a.count > 0 }

func (a *assembler) body() []byte {
	return a.content[:(a.count-1)*partContentLen+a.lastLength]
}

func (a *assembler) add(f frame) assembly {
	if f.partCount < 2 || f.partCount > maxHandshakeParts || f.partIndex >= f.partCount {
		return assemblyBroken
	}
	last := f.partIndex == f.partCount-1
	if last && len(f.payload) == 0 || !last && len(f.payload) != partContentLen {
		return assemblyBroken
	}

	if a.count == 0 {
		a.begin(f.partCount)
	} else if f.partCount != a.count {
		return assemblyBroken
	}

	offset := f.partIndex * partContentLen
	if a.received[f.partIndex] {
		if last && len(f.payload) != a.lastLength {
			return assemblyBroken
		}
		if !bytes.Equal(f.payload, a.content[offset:offset+len(f.payload)]) {
			return assemblyBroken
		}
		return assemblyPending
	}

	copy(a.content[offset:], f.payload)
	a.received[f.partIndex] = true
	a.arrived++
	if last {
		a.lastLength = len(f.payload)
	}

	if a.arrived < a.count {
		return assemblyPending
	}
	if len(a.body()) > MaxHandshakePayload {
		return assemblyBroken
	}
	return assemblyComplete
}

func (a *assembler) reset() {
	clear(a.received[:a.count])
	a.count = 0
	a.arrived = 0
	a.lastLength = 0
}

func (a *assembler) shrink() {
	if a.count == 0 {
		a.content = nil
		a.received = nil
	}
}

func (a *assembler) begin(count int) {
	if capacity := count * partContentLen; len(a.content) < capacity {
		a.content = make([]byte, capacity)
	}
	if len(a.received) < count {
		a.received = make([]bool, count)
	}
	a.count = count
}
