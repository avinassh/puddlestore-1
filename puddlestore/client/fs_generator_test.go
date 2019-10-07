package client

import (
	"fmt"
	"testing"

	"github.com/brown-csci1380/s18-mcisler-vmathur2/puddlestore/puddlestore"
)

const NUM_ITERATIONS = 25
const HANG_TO_VIEW = false
const SUPRESS_OUTPUT = true

func TestRandomFS(t *testing.T) {
	fs := InitFileSystem(t, 3)
	if fs == nil {
		return
	}
	puddlestore.SuppressLoggers(SUPRESS_OUTPUT)

	t.Run("generate step", func(t *testing.T) {
		GenerateFS(fs, t, NUM_ITERATIONS)
	})

	t.Run("mutate and maintain step", func(t *testing.T) {
		MutateFS(fs, t, NUM_ITERATIONS, true)
	})

	t.Run("mutate and delete step", func(t *testing.T) {
		MutateFS(fs, t, NUM_ITERATIONS, false)
	})

	for HANG_TO_VIEW {
		fmt.Print("")
	}
}
