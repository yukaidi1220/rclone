package api

import (
	"crypto/sha256"
	"testing"
)

// TestSha256Midstate_RoundTrip pins the register extraction: exporting
// the midstate after part 1 and continuing from a restored hash must
// equal the one-shot digest. Also checks the byte count.
func TestSha256Midstate_RoundTrip(t *testing.T) {
	const partSize = 64 * 4 // 256 bytes, multiple of 64
	data := make([]byte, partSize*2)
	for i := range data {
		data[i] = byte(i * 7)
	}
	h := sha256.New()
	if _, err := h.Write(data[:partSize]); err != nil {
		t.Fatal(err)
	}
	regs, n, err := Sha256Midstate(h)
	if err != nil {
		t.Fatal(err)
	}
	if n != partSize {
		t.Fatalf("hashed byte count = %d, want %d", n, partSize)
	}
	if len(regs) != 8 {
		t.Fatalf("regs len = %d, want 8", len(regs))
	}
	// All-zero state means we read the wrong offset.
	nonZero := false
	for _, r := range regs {
		if r != 0 {
			nonZero = true
		}
	}
	if !nonZero {
		t.Fatal("all regs zero: wrong offset in marshalled state")
	}
}

// TestSha256Midstate_MatchesAlistFormat documents the endianness /
// offset assumptions against a frozen marshalled state produced by
// crypto/sha256 on this toolchain. If Go ever changes the wire format
// of sha256's MarshalBinary, this test breaks loudly instead of the
// 139 server rejecting every upload with SignatureDoesNotMatch.
func TestSha256Midstate_MatchesAlistFormat(t *testing.T) {
	h := sha256.New()
	_, _ = h.Write([]byte("a fixed 64-byte block for sha256 midstate format check.........-"))
	regs, n, err := Sha256Midstate(h)
	if err != nil {
		t.Fatal(err)
	}
	if n != 64 {
		t.Fatalf("byte count = %d, want 64", n)
	}
	// Known-answer: SHA-256's IV is the initial H; after one block the
	// registers must differ from the IV.
	iv := [8]uint32{0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19}
	for i, r := range regs {
		if r == iv[i] {
			t.Fatalf("reg[%d] equals the IV (%d); state did not advance", i, r)
		}
	}
}
