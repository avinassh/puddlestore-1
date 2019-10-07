package client

import (
	"math/rand"
	"testing"
	"time"

	"fmt"

	"math"

	ps "github.com/brown-csci1380/s18-mcisler-vmathur2/puddlestore/puddlestore"
	"github.com/stretchr/testify/assert"
)

const RAND_FILE_DATA_LEN = 10
const RAND_FILENAME_WORD_LEN = 3

/************************************************************************************/
// FS Oracles
/************************************************************************************/

/*
	Given a PuddleStore filesystem, generates a random set of files and directories.
	Optionally performs assertions on that system given non-nil testing object.
*/
func GenerateFS(state *FileSystemState, t *testing.T, iterations int) {
	rand.Seed(time.Now().UTC().UnixNano())
	for i := 0; i < iterations; i++ {
		t.Run(fmt.Sprintf("run_%d", i), func(t *testing.T) {
			RandomGenerate(state, t)
		})
	}
}

func RandomGenerate(state *FileSystemState, t *testing.T) {
	if ps.WithProb(0.2) {
		cdUp(state, t)
	}
	if ps.WithProb(0.3) {
		randCd(state, t)
	}
	if ps.WithProb(0.7) {
		randMkdir(state, t)
	}
	if ps.WithProb(0.7) {
		randTouch(state, t)
	}
}

/*
	Given a PuddleStore filesystem, randomly adds, removes, and edits the files and directories present.
	Optionally performs assertions on that system given non-nil testing object.
*/
func MutateFS(state *FileSystemState, t *testing.T, iterations int, createNew bool) {
	rand.Seed(time.Now().UTC().UnixNano())
	for i := 0; i < iterations; i++ {
		t.Run(fmt.Sprintf("run_%d", i), func(t *testing.T) {
			RandomMutate(state, t, createNew)
		})
	}
}

func RandomMutate(state *FileSystemState, t *testing.T, createNew bool) {
	if ps.WithProb(0.3) {
		cdUp(state, t)
	}
	if ps.WithProb(0.5) {
		randCd(state, t)
	}
	if createNew {
		if ps.WithProb(0.7) {
			randMkdir(state, t)
		}
		if ps.WithProb(0.7) {
			randTouch(state, t)
		}
	}
	if ps.WithProb(0.8) {
		randEdit(state, t)
	}
	if ps.WithProb(0.6) {
		randRm(state, t)
	}
}

/************************************************************************************/
// Command wrappers
/************************************************************************************/
func cdUp(state *FileSystemState, t *testing.T) {
	if !state.Path.IsRoot() {
		prevDir := state.CurrentDir
		newDir, err := state.Cd("..")
		if t != nil {
			t.Run("cdUp", func(t *testing.T) {
				assert.NoError(t, err)
				if newDir != nil {
					assert.NotEqual(t, prevDir.GetAGUID(), newDir.GetAGUID())
					assert.NotEqual(t, prevDir.GetName(), newDir.GetName())
				}
			})
		}
	}
}

func randCd(state *FileSystemState, t *testing.T) {
	chosenDir := getRandChildInode(state, ps.DIRECTORY)
	if chosenDir != nil {
		newDir, err := state.Cd(chosenDir.GetName())
		if t != nil {
			t.Run(fmt.Sprintf("cd_%v", chosenDir.GetName()), func(t *testing.T) {
				assert.NoError(t, err)
				if newDir != nil {
					assert.Equal(t, newDir.GetAGUID(), chosenDir.GetAGUID())
					// VGUID not necessarily same (refresh)
					assert.Equal(t, newDir.GetName(), chosenDir.GetName())
					assert.Equal(t, newDir.GetSize(), chosenDir.GetSize())
				}
			})
		}
	}
}

func randMkdir(state *FileSystemState, t *testing.T) {
	prevCurDir := state.CurrentDir.Clone()
	randName := ps.GetRandName(RAND_FILENAME_WORD_LEN)
	newDir, err := state.Mkdir(randName)
	if t != nil {
		t.Run(fmt.Sprintf("mkdir_%v", randName), func(t *testing.T) {
			assert.NoError(t, err)
			if newDir != nil {
				assert.Equal(t, randName, newDir.GetName())
				assert.Equal(t, uint64(0), newDir.GetSize())
				assert.Equal(t, uint(ps.DIRECTORY), newDir.Class)
				assert.Equal(t, 0, len(newDir.Blocks))

				// parent dir checks
				assert.Contains(t, state.CurrentDir.Blocks, newDir.GetAGUID())
				assert.Equal(t, prevCurDir.GetAGUID(), state.CurrentDir.GetAGUID())
				assert.NotEqual(t, prevCurDir.GetVGUID(), state.CurrentDir.GetVGUID())
				assert.Equal(t, prevCurDir.GetSize()+1, state.CurrentDir.GetSize())
				assert.Equal(t, len(prevCurDir.Blocks)+1, len(state.CurrentDir.Blocks))
			}
		})
	}
}

func randTouch(state *FileSystemState, t *testing.T) {
	prevCurDir := state.CurrentDir.Clone()
	randName := ps.GetRandName(RAND_FILENAME_WORD_LEN)
	newFile, err := state.Touch(randName)
	if t != nil {
		t.Run(fmt.Sprintf("touch_%v", randName), func(t *testing.T) {
			assert.NoError(t, err)
			if newFile != nil {
				assert.Equal(t, randName, newFile.GetName())
				assert.Equal(t, uint64(0), newFile.GetSize())
				assert.Equal(t, uint(ps.FILE), newFile.Class)
				assert.Equal(t, int(0), len(newFile.Blocks))

				// parent dir checks
				assert.Contains(t, state.CurrentDir.Blocks, newFile.GetAGUID())
				assert.Equal(t, prevCurDir.GetAGUID(), state.CurrentDir.GetAGUID())
				assert.NotEqual(t, prevCurDir.GetVGUID(), state.CurrentDir.GetVGUID())
				assert.Equal(t, prevCurDir.GetSize()+1, state.CurrentDir.GetSize())
				assert.Equal(t, len(prevCurDir.Blocks)+1, len(state.CurrentDir.Blocks))
			}
		})
	}
}

func randEdit(state *FileSystemState, t *testing.T) {
	prevCurDir := state.CurrentDir.Clone()
	randChild := getRandChildInode(state, ps.FILE)
	if randChild != nil {
		randData := []byte(ps.GetRandData(RAND_FILE_DATA_LEN))
		retFile, err := state.WriteTo(randChild.GetName(), randData)
		if t != nil {
			t.Run(fmt.Sprintf("edit_%v", randChild.GetName()), func(t *testing.T) {
				assert.NoError(t, err)
				if retFile != nil {
					assert.Equal(t, randChild.GetAGUID(), retFile.GetAGUID())
					assert.NotEqual(t, randChild.GetVGUID(), retFile.GetVGUID())
					assert.Equal(t, randChild.GetName(), retFile.GetName())
					assert.Equal(t, uint64(len(randData)), retFile.GetSize())
					assert.Equal(t, randChild.Class, retFile.Class)
					// need enough blocks to fit all the data, so do ceiling, but if it fits perfectly we're fine
					if len(randData)%ps.DBLOCKSIZE == 0 {
						assert.True(t, len(randData)/ps.DBLOCKSIZE <= len(retFile.Blocks))
					} else {
						assert.True(t, int(math.Ceil(float64(len(randData))/float64(ps.DBLOCKSIZE))) <= len(retFile.Blocks),
							"len(randData): %d, len(retFile.Blocks): %d, LHS: %d",
							len(randData), len(retFile.Blocks), int(math.Ceil(float64(len(randData))/float64(ps.DBLOCKSIZE))))
					}

					// parent dir checks (no change)
					assert.Contains(t, state.CurrentDir.Blocks, retFile.GetAGUID())
					assert.Equal(t, prevCurDir.GetAGUID(), state.CurrentDir.GetAGUID())
					assert.Equal(t, prevCurDir.GetVGUID(), state.CurrentDir.GetVGUID())
					assert.Equal(t, prevCurDir.GetSize(), state.CurrentDir.GetSize())
					assert.Equal(t, len(prevCurDir.Blocks), len(state.CurrentDir.Blocks))
				}
			})
		}
	}
}

func randRm(state *FileSystemState, t *testing.T) {
	prevCurDir := state.CurrentDir.Clone()
	randChild := getRandChildInode(state, -1)
	if randChild != nil {
		err := state.Rm(randChild.GetName())
		if t != nil {
			t.Run(fmt.Sprintf("rm_%v", randChild.GetName()), func(t *testing.T) {
				assert.NoError(t, err)
				// parent dir checks
				assert.NotContains(t, state.CurrentDir.Blocks, randChild.GetAGUID())
				assert.Equal(t, prevCurDir.GetAGUID(), state.CurrentDir.GetAGUID())
				assert.NotEqual(t, prevCurDir.GetVGUID(), state.CurrentDir.GetVGUID())
				assert.Equal(t, prevCurDir.GetSize()-1, state.CurrentDir.GetSize())
				assert.Equal(t, len(prevCurDir.Blocks)-1, len(state.CurrentDir.Blocks))
			})
		}
	}
}

/************************************************************************************/
// Helpers
/************************************************************************************/

// Returns a random Inode in the current dir of the given class, or any inode if class < 0
func getRandChildInode(state *FileSystemState, class int) *ps.INode {
	inodes := make([]*ps.INode, 0)
	for i := 0; i < int(state.CurrentDir.GetSize()); i++ {
		inode, _ := state.CurrentDir.GetChildAt(i)
		if class < 0 || inode.Class == uint(class) {
			inodes = append(inodes, inode)
		}
	}
	if len(inodes) > 0 {
		return inodes[rand.Intn(len(inodes))]
	}
	return nil
}
