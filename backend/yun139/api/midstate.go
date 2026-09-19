// Package api — sha256 midstate helpers for 139 parallel upload.
//
// 139's /hcy/file/create accepts parallelUpload:true when every part
// after the first carries a parallelHashCtx: the SHA-256 internal state
// (8 uint32 H words) after hashing all bytes that precede that part.
// The server signs the context into each part's upload URL
// (X-Amz-Iteration-Hash-Ctx), which is what allows out-of-order
// concurrent part PUTs.
package api

import (
	"encoding"
	"encoding/binary"
	"fmt"
)

// sha256MidstateLen is the minimum length of crypto/sha256's
// marshalled state: 4-byte magic "sha\x03" + 8 H registers + trailing
// 8-byte length.
const sha256MidstateLen = 4 + 8*4 + 8

// Sha256Midstate returns the 8 uint32 H registers of the SHA-256 state
// after the bytes fed so far, plus how many bytes that is.
//
// H is read big-endian, matching crypto/sha256's MarshalBinary format
// (verified against alist's upload_parallel.go which was verified
// against the live server).
func Sha256Midstate(h interface {
	Write(p []byte) (int, error)
	Sum(b []byte) []byte
}) ([]uint32, int64, error) {
	m, ok := h.(encoding.BinaryMarshaler)
	if !ok {
		return nil, 0, fmt.Errorf("sha256 hash state is not exportable")
	}
	state, err := m.MarshalBinary()
	if err != nil {
		return nil, 0, err
	}
	if len(state) < sha256MidstateLen {
		return nil, 0, fmt.Errorf("unexpected sha256 state length %d", len(state))
	}
	regs := make([]uint32, 8)
	for i := range regs {
		regs[i] = binary.BigEndian.Uint32(state[4+i*4:])
	}
	return regs, int64(binary.BigEndian.Uint64(state[len(state)-8:])), nil
}
