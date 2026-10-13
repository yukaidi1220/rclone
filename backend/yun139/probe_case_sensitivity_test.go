package yun139

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestProbeCaseSensitivityTwoSpellings measures which name pairs the personal
// space treats as one entry. It decides how far a backup-name match may fold
// case: folding a pair the server keeps apart would aim the cleanup of one
// leaf at the live content of another.
func TestProbeCaseSensitivityTwoSpellings(t *testing.T) {
	probeSkip(t)
	f := probeFs(t, probeRemote())
	ctx := context.Background()
	base := probeBase()

	// Each pair is written lower then upper; one entry afterwards means the
	// server treats the two spellings as the same name.
	for _, pair := range [][2]string{
		{"casecheck.txt", "CaseCheck.txt"},
		{"ünïcode.txt", "Ünïcode.txt"},
		{"\u212aelvin.txt", "Kelvin.txt"},
	} {
		lower, upper := pair[0], pair[1]
		ids := map[string]string{}
		probePut(ctx, t, f, base, lower, "lower-content-1")
		time.Sleep(5 * time.Second)
		t.Logf("%s: after lower %v", lower, probeListNames(ctx, t, f, base))

		probePut(ctx, t, f, base, upper, "upper-content-1")
		time.Sleep(5 * time.Second)
		t.Logf("%s/%s: after upper %v", lower, upper, probeListNames(ctx, t, f, base))

		for _, n := range []string{lower, upper} {
			o, err := f.NewObject(ctx, base+"/"+n)
			require.NoError(t, err, "read %s", n)
			ids[n] = o.(*Object).id
		}
		// Both spellings must resolve to one entry: that is the assumption
		// isBackupOf's case fold rests on. A server that starts keeping them
		// apart makes the fold unsafe, and this fails.
		assert.Equal(t, ids[lower], ids[upper],
			"%s and %s must name the same entry", lower, upper)
	}
}
