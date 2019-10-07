package client

import (
	"fmt"
	"testing"

	"github.com/brown-csci1380/s18-mcisler-vmathur2/puddlestore/puddlestore"
	"github.com/stretchr/testify/assert"
)

func InitFileSystem(t *testing.T, tapestrySize int) (state *FileSystemState) {
	puddlestore.RemoveRaftLogs()
	puddlestore.StartPuddleStoreCluster(puddlestore.DEFAULT_ZK_ADDR, tapestrySize)
	fs, err := NewFileSystem(puddlestore.DEFAULT_ZK_ADDR)
	if err != nil || fs == nil || fs.CurrentDir == nil {
		t.Errorf("%v", err)
		if !assert.NotNil(t, fs) {
			return nil
		}
		assert.NotNil(t, fs.CurrentDir)
		return nil
	}
	return fs
}

func TestScp(t *testing.T) {
	fs := InitFileSystem(t, 3)
	if fs == nil {
		return
	}
	defer fs.Close()

	// d8b3a472-7ddb-4df7-9bc2-526b998def0b
	t.Log("Successfully started filesystem")
	fs.Scp("client_test.go", "smol")
	fs.Ls()
	t.Log(fs.Cat("smol"))

}

func TestBasicFileReadWrite(t *testing.T) {
	fs := InitFileSystem(t, 3)
	if fs == nil {
		return
	}
	defer fs.Close()

	t.Run("ls with nothing", func(t *testing.T) {
		contents, err := fs.Ls()
		assert.NoError(t, err)
		if err != nil {
			return
		}
		assert.Equal(t, []string{}, contents)
	})

	t.Run("touch file", func(t *testing.T) {
		file, err := fs.Touch("dog")
		assert.NoError(t, err)
		if err != nil {
			return
		}
		assert.Equal(t, uint64(0), file.GetSize())

		contents, err := fs.Ls()
		assert.NoError(t, err)
		if err != nil {
			return
		}
		assert.Equal(t, []string{"dog"}, contents)

		data, err := fs.Cat("dog")
		assert.NoError(t, err)
		if err != nil {
			return
		}
		assert.Equal(t, "", data)
	})

	t.Run("write to new file", func(t *testing.T) {
		data := []byte("cats are cool")
		file, err := fs.WriteTo("dog", data)
		assert.NoError(t, err)
		if err != nil {
			return
		}
		assert.Equal(t, uint64(len(data)), file.GetSize())

		dataFrom, err := fs.Cat("dog")
		assert.NoError(t, err)
		assert.Equal(t, "cats are cool", dataFrom)
	})

	t.Run("write to existing file", func(t *testing.T) {
		data := []byte("the cats are cool")
		file, err := fs.WriteTo("dog", data)
		assert.NoError(t, err)
		if err != nil {
			return
		}
		assert.Equal(t, uint64(len(data)), file.GetSize())

		dataFrom, err := fs.Cat("dog")
		assert.NoError(t, err)
		if err != nil {
			return
		}
		assert.Equal(t, "the cats are cool", dataFrom)
	})

	t.Run("write to existing file reduce", func(t *testing.T) {
		data := []byte("lolz")
		file, err := fs.WriteTo("dog", data)
		assert.NoError(t, err)
		if err != nil {
			return
		}
		assert.Equal(t, uint64(len(data)), file.GetSize())

		dataFrom, err := fs.Cat("dog")
		assert.NoError(t, err)
		if err != nil {
			return
		}
		assert.Equal(t, "lolz", dataFrom)
	})
}

func assertNewDir(t *testing.T, fs *FileSystemState, newDir *puddlestore.Directory, name string) {
	assert.NotNil(t, newDir)
	if newDir != nil {
		t.Run(fmt.Sprintf("new dir assertions %v", newDir.GetName()), func(t *testing.T) {
			assert.Equal(t, name, newDir.GetName())
			assert.Equal(t, uint64(0), newDir.GetSize())
			assert.Len(t, newDir.Blocks, 0)
		})
	}
}

func TestBasicDirectoryNavigation(t *testing.T) {
	fs := InitFileSystem(t, 3)
	if fs == nil {
		return
	}
	defer fs.Close()

	t.Run("ls with nothing", func(t *testing.T) {
		contents, err := fs.Ls()
		assert.NoError(t, err)
		if err != nil {
			return
		}
		assert.Equal(t, []string{}, contents)
	})

	// create basic directory
	t.Run("create files in root", func(t *testing.T) {
		prevVGUID := fs.CurrentDir.GetVGUID()
		_, err := fs.Mkdir("cats")
		assert.NoError(t, err)
		_, err = fs.Mkdir("dogs")
		assert.NoError(t, err)
		_, err = fs.Mkdir("lolz")
		assert.NoError(t, err)

		assert.NotEqual(t, prevVGUID, fs.CurrentDir.GetVGUID())
		assert.Equal(t, uint64(3), fs.CurrentDir.GetSize())
		assert.Len(t, fs.CurrentDir.Blocks, 3)

		contents, err := fs.Ls()
		assert.NoError(t, err)
		if err != nil {
			return
		}
		assert.ElementsMatch(t, []string{"cats", "dogs", "lolz"}, contents)
	})

	t.Run("create files in subdir", func(t *testing.T) {
		newDir, err := fs.Cd("cats")
		assert.NoError(t, err)
		if err != nil {
			return
		}
		assertNewDir(t, fs, newDir, "cats")
		assert.Equal(t, fs.CurrentDir, newDir)
		prevVGUID := newDir.GetVGUID()

		_, err = fs.Mkdir("you")
		assert.NoError(t, err)
		_, err = fs.Mkdir("know")
		assert.NoError(t, err)
		_, err = fs.Mkdir("this")
		assert.NoError(t, err)
		_, err = fs.Mkdir("test")
		assert.NoError(t, err)
		_, err = fs.Mkdir("is")
		assert.NoError(t, err)
		_, err = fs.Mkdir("great")
		assert.NoError(t, err)

		assert.NotEqual(t, prevVGUID, fs.CurrentDir.GetVGUID())
		assert.Equal(t, uint64(6), fs.CurrentDir.GetSize())
		assert.Len(t, fs.CurrentDir.Blocks, 6)
	})

	t.Run("go deep", func(t *testing.T) {
		newDir, err := fs.Cd("this")
		assert.NoError(t, err)
		if err != nil {
			return
		}
		assertNewDir(t, fs, newDir, "this")
		assert.Equal(t, fs.CurrentDir, newDir)

		newDir, err = fs.Mkdir("and")
		assert.NoError(t, err)
		assertNewDir(t, fs, newDir, "and")
		newDir, err = fs.Cd("and")
		assert.Equal(t, fs.CurrentDir, newDir)

		newDir, err = fs.Mkdir("way")
		assert.NoError(t, err)
		assertNewDir(t, fs, newDir, "way")
		newDir, err = fs.Cd("way")
		assert.Equal(t, fs.CurrentDir, newDir)

		newDir, err = fs.Mkdir("down")
		assert.NoError(t, err)
		assertNewDir(t, fs, newDir, "down")
		newDir, err = fs.Cd("down")
		assert.Equal(t, fs.CurrentDir, newDir)

		newDir, err = fs.Mkdir("we")
		assert.NoError(t, err)
		assertNewDir(t, fs, newDir, "we")
		newDir, err = fs.Cd("we")
		assert.Equal(t, fs.CurrentDir, newDir)

		newDir, err = fs.Mkdir("go")
		assert.NoError(t, err)
		assertNewDir(t, fs, newDir, "go")
		newDir, err = fs.Cd("go")
		assert.Equal(t, fs.CurrentDir, newDir)

	})

	t.Run("cd all over", func(t *testing.T) {
		_, err := fs.Cd("..")
		assert.NoError(t, err)
		assert.Equal(t, "we", fs.CurrentDir.GetName())
		assert.Equal(t, uint64(1), fs.CurrentDir.GetSize())
		contents, err := fs.Ls()
		assert.NoError(t, err)
		assert.ElementsMatch(t, []string{"go"}, contents)

		_, err = fs.Cd("..")
		assert.NoError(t, err)
		assert.Equal(t, "down", fs.CurrentDir.GetName())
		assert.Equal(t, uint64(1), fs.CurrentDir.GetSize())
		contents, err = fs.Ls()
		assert.NoError(t, err)
		assert.ElementsMatch(t, []string{"we"}, contents)

		_, err = fs.Cd("..")
		assert.NoError(t, err)
		assert.Equal(t, "way", fs.CurrentDir.GetName())
		assert.Equal(t, uint64(1), fs.CurrentDir.GetSize())
		newDir, err := fs.Mkdir("psych")
		assert.Equal(t, uint64(2), fs.CurrentDir.GetSize())
		assertNewDir(t, fs, newDir, "psych")
		assert.NoError(t, err)
		contents, err = fs.Ls()
		assert.NoError(t, err)
		assert.ElementsMatch(t, []string{"down", "psych"}, contents)
		newDir, err = fs.Cd("psych")
		assertNewDir(t, fs, newDir, "psych")
		assert.NoError(t, err)
		_, err = fs.Cd("..")
		assert.NoError(t, err)

		_, err = fs.Cd("..")
		assert.NoError(t, err)
		assert.Equal(t, "and", fs.CurrentDir.GetName())
		assert.Equal(t, uint64(1), fs.CurrentDir.GetSize())

		_, err = fs.Cd("..")
		assert.NoError(t, err)
		assert.Equal(t, "this", fs.CurrentDir.GetName())
		assert.Equal(t, uint64(1), fs.CurrentDir.GetSize())

		_, err = fs.Cd("..")
		assert.NoError(t, err)
		assert.Equal(t, "cats", fs.CurrentDir.GetName())
		assert.Equal(t, uint64(6), fs.CurrentDir.GetSize())
		contents, err = fs.Ls()
		assert.NoError(t, err)
		assert.ElementsMatch(t, []string{"you", "know", "this", "test", "is", "great"}, contents)

		_, err = fs.Cd("..")
		assert.NoError(t, err)
		assert.Equal(t, puddlestore.NAME_OF_ROOT_DIR, fs.CurrentDir.GetName())
		assert.Equal(t, uint64(3), fs.CurrentDir.GetSize())
	})
}

func (state *FileSystemState) createBasicDirectoryStructure(t *testing.T) {
	// /cats/
	_, err := state.Mkdir("cats")
	assert.NoError(t, err)
	newDir, err := state.Cd("cats/")
	assert.Equal(t, state.CurrentDir, newDir)

	// /cats/feline.jpg
	_, err = state.Touch("feline.jpg")
	assert.NoError(t, err)
	// /cats/cool
	_, err = state.Mkdir("cool")
	assert.NoError(t, err)
	// /cats/lolz
	_, err = state.Mkdir("lolz")
	assert.NoError(t, err)
	newDir, err = state.Cd("lolz/")
	assert.Equal(t, state.CurrentDir, newDir)

	// /cats/lolz/memes
	_, err = state.Mkdir("memes")
	assert.NoError(t, err)
	// /cats/lolz/more memes
	_, err = state.Mkdir("more memes")
	assert.NoError(t, err)
	// /cats/lolz/actual memes.txt
	_, err = state.Touch("actual memes.txt")
	assert.NoError(t, err)

	newDir, err = state.Cd("..")
	assert.Equal(t, state.CurrentDir, newDir)
	newDir, err = state.Cd("cool")
	assert.Equal(t, state.CurrentDir, newDir)
	// /cats/cool/fun/
	_, err = state.Mkdir("fun")
	assert.NoError(t, err)
	newDir, err = state.Cd("fun")
	assert.NoError(t, err)
	// /cats/cool/fun/really fun
	_, err = state.Mkdir("really fun")
	assert.NoError(t, err)
	newDir, err = state.Cd("really fun")
	assert.NoError(t, err)
	// /cats/cool/fun/really fun/huh.txt
	_, err = state.Mkdir("huh.txt")
	assert.NoError(t, err)
	// /cats/cool/fun/really fun/even more fun/
	_, err = state.Mkdir("even more fun")
	assert.NoError(t, err)

	newDir, err = state.Cd("..")
	assert.NoError(t, err)
	newDir, err = state.Cd("..")
	assert.NoError(t, err)
	newDir, err = state.Cd("..")
	assert.NoError(t, err)
	newDir, err = state.Cd("..")
	assert.NoError(t, err)

	// /dogs/
	_, err = state.Mkdir("dogs")
	assert.NoError(t, err)
	// /dogs/mice.txt
	_, err = state.Touch("mice.txt")
	assert.NoError(t, err)
}

func TestAbsolutePaths(t *testing.T) {
	fs := InitFileSystem(t, 3)
	if fs == nil {
		return
	}
	defer fs.Close()

	t.Run("create directory structure", fs.createBasicDirectoryStructure)

	t.Run("cd absolute", func(t *testing.T) {
		dir, err := fs.Cd("/")
		assert.NoError(t, err)
		if dir != nil {
			assert.Equal(t, "root", dir.GetName())
		}

		dir, err = fs.Cd("/cats")
		assert.NoError(t, err)
		if dir != nil {
			assert.Equal(t, "cats", dir.GetName())
		}

		dir, err = fs.Cd("/cats/cool/")
		assert.NoError(t, err)
		if dir != nil {
			assert.Equal(t, "cool", dir.GetName())
		}

		dir, err = fs.Cd("/cats/lolz")
		assert.NoError(t, err)
		if dir != nil {
			assert.Equal(t, "lolz", dir.GetName())
		}

		dir, err = fs.Cd("/cats/lolz/more memes")
		assert.NoError(t, err)
		if dir != nil {
			assert.Equal(t, "more memes", dir.GetName())
		}

		dir, err = fs.Cd("/cats/cool///fun/really fun//even more fun/")
		assert.NoError(t, err)
		if dir != nil {
			assert.Equal(t, "even more fun", dir.GetName())
		}
		dir, err = fs.Cd("..")
		assert.NoError(t, err)
		if dir != nil {
			assert.Equal(t, "really fun", dir.GetName())
		}

		dir, err = fs.Cd("/cats/cool/fun/really fun")
		assert.NoError(t, err)
		if dir != nil {
			assert.Equal(t, "really fun", dir.GetName())
		}
	})
}

func TestRelativePaths(t *testing.T) {
	fs := InitFileSystem(t, 3)
	if fs == nil {
		return
	}
	defer fs.Close()

	t.Run("create directory structure", fs.createBasicDirectoryStructure)

	t.Run("cd relative", func(t *testing.T) {
		dir, err := fs.Cd("cats")
		assert.NoError(t, err)
		if dir != nil {
			assert.Equal(t, "cats", dir.GetName())
		}

		dir, err = fs.Cd("cool//")
		assert.NoError(t, err)
		if dir != nil {
			assert.Equal(t, "cool", dir.GetName())
		}

		dir, err = fs.Cd("fun/really fun/even more fun")
		assert.NoError(t, err)
		if dir != nil {
			assert.Equal(t, "even more fun", dir.GetName())
		} else {
			t.Errorf("%v", fs.Path)
		}
		dir, err = fs.Cd("..")
		assert.NoError(t, err)
		if dir != nil {
			assert.Equal(t, "really fun", dir.GetName())
		}

		dir, err = fs.Cd("even more fun")
		assert.NoError(t, err)
		if dir != nil {
			assert.Equal(t, "even more fun", dir.GetName())
		}

		dir, err = fs.Cd("..")
		assert.NoError(t, err)
		dir, err = fs.Cd("..")
		assert.NoError(t, err)
		dir, err = fs.Cd("..")
		assert.NoError(t, err)
		dir, err = fs.Cd("..")
		assert.NoError(t, err)

		dir, err = fs.Cd("lolz/more memes")
		assert.NoError(t, err)
		if dir != nil {
			assert.Equal(t, "more memes", dir.GetName())
		}
	})
}

func assertNewFileExists(t *testing.T, fs *FileSystemState, newFile *puddlestore.File, name string, expectedContents []string) {
	assert.NotNil(t, newFile)
	if newFile != nil {
		assert.Equal(t, name, newFile.GetName())
		assert.Equal(t, uint64(0), newFile.GetSize())

		assert.Equal(t, uint64(len(expectedContents)), fs.CurrentDir.GetSize())
		assert.Len(t, fs.CurrentDir.Blocks, len(expectedContents))

		contents, err := fs.Ls()
		assert.NoError(t, err)
		assert.ElementsMatch(t, expectedContents, contents)
	}
}

func TestBasicRemoval(t *testing.T) {
	fs := InitFileSystem(t, 3)
	if fs == nil {
		return
	}
	defer fs.Close()

	file, err := fs.Touch("sneaky")
	assert.NoError(t, err)
	assertNewFileExists(t, fs, file, "sneaky", []string{"sneaky"})

	t.Run("remove blank file", func(t *testing.T) {
		// make sure created
		prevVGUID := fs.CurrentDir.GetVGUID()
		file, err := fs.Touch("cats")
		assert.NoError(t, err)
		assertNewFileExists(t, fs, file, "cats", []string{"cats", "sneaky"})
		assert.NotEqual(t, prevVGUID, fs.CurrentDir.GetVGUID())
		prevVGUID = fs.CurrentDir.GetVGUID()

		// now remove
		fs.Rm("cats")
		assert.NotEqual(t, prevVGUID, fs.CurrentDir.GetVGUID())
		assert.Equal(t, uint64(1), fs.CurrentDir.GetSize())
		assert.Len(t, fs.CurrentDir.Blocks, 1)
		_, err = fs.Cat("boo")
		assert.Error(t, err)

		contents, err := fs.Ls()
		assert.NoError(t, err)
		assert.ElementsMatch(t, []string{"sneaky"}, contents)
	})

	t.Run("remove full file", func(t *testing.T) {
		// make sure created
		prevVGUID := fs.CurrentDir.GetVGUID()
		file, err := fs.Touch("dogs")
		assert.NoError(t, err)
		assertNewFileExists(t, fs, file, "dogs", []string{"dogs", "sneaky"})
		randData := []byte(puddlestore.GetRandData(100))
		file, err = fs.WriteTo("dogs", randData)
		assert.NoError(t, err)
		assert.Equal(t, uint64(len(randData)), file.GetSize())
		assert.NotEqual(t, prevVGUID, fs.CurrentDir.GetVGUID())
		prevVGUID = fs.CurrentDir.GetVGUID()

		// now remove
		fs.Rm("dogs")
		assert.NotEqual(t, prevVGUID, fs.CurrentDir.GetVGUID())
		assert.Equal(t, uint64(1), fs.CurrentDir.GetSize())
		assert.Len(t, fs.CurrentDir.Blocks, 1)
		_, err = fs.Cat("boo")
		assert.Error(t, err)

		contents, err := fs.Ls()
		assert.NoError(t, err)
		assert.ElementsMatch(t, []string{"sneaky"}, contents)
	})

	t.Run("remove blank directory", func(t *testing.T) {
		// make sure created
		prevVGUID := fs.CurrentDir.GetVGUID()
		file, err := fs.Mkdir("boo")
		assert.NoError(t, err)
		assertNewDir(t, fs, file, "boo")
		assert.NotEqual(t, prevVGUID, fs.CurrentDir.GetVGUID())
		prevVGUID = fs.CurrentDir.GetVGUID()

		// now remove
		fs.Rm("boo")
		assert.NotEqual(t, prevVGUID, fs.CurrentDir.GetVGUID())
		assert.Equal(t, uint64(1), fs.CurrentDir.GetSize())
		assert.Len(t, fs.CurrentDir.Blocks, 1)
		_, err = fs.Cd("boo")
		assert.Error(t, err)

		contents, err := fs.Ls()
		assert.NoError(t, err)
		assert.ElementsMatch(t, []string{"sneaky"}, contents)
	})

}

func TestRecursiveRemoval(t *testing.T) {
	fs := InitFileSystem(t, 3)
	if fs == nil {
		return
	}
	defer fs.Close()

	t.Run("remove full directory", func(t *testing.T) {
		// make sure created
		prevVGUID := fs.CurrentDir.GetVGUID()
		file, err := fs.Mkdir("zoo")
		assert.NoError(t, err)
		assertNewDir(t, fs, file, "zoo")
		assert.NotEqual(t, prevVGUID, fs.CurrentDir.GetVGUID())
		prevVGUID = fs.CurrentDir.GetVGUID()

		// add stuff
		_, err = fs.Cd("zoo")
		assert.NoError(t, err)
		_, err = fs.Touch("lols")
		assert.NoError(t, err)
		_, err = fs.Mkdir("directory!!!")
		assert.NoError(t, err)
		_, err = fs.Touch("file!!!")
		assert.NoError(t, err)
		_, err = fs.Cd("..")
		assert.NoError(t, err)

		// now remove
		fs.Rm("zoo")
		assert.NotEqual(t, prevVGUID, fs.CurrentDir.GetVGUID())
		assert.Equal(t, uint64(0), fs.CurrentDir.GetSize())
		assert.Len(t, fs.CurrentDir.Blocks, 0)
		_, err = fs.Cd("zoo")
		assert.Error(t, err)

		contents, err := fs.Ls()
		assert.NoError(t, err)
		assert.Len(t, contents, 0)
	})

	t.Run("remove nested", func(t *testing.T) {
		prevVGUID := fs.CurrentDir.GetVGUID()

		newDir, err := fs.Mkdir("you")
		assert.NoError(t, err)
		assertNewDir(t, fs, newDir, "you")
		newFile, err := fs.Touch("know")
		assert.NoError(t, err)
		assertNewFileExists(t, fs, newFile, "know", []string{"you", "know"})

		// cd down
		_, err = fs.Cd("you")
		assert.NoError(t, err)
		newDir, err = fs.Mkdir("this")
		assert.NoError(t, err)
		assertNewDir(t, fs, newDir, "this")
		newFile, err = fs.Touch("is")
		assert.NoError(t, err)
		assertNewFileExists(t, fs, newFile, "is", []string{"this", "is"})
		newDir, err = fs.Mkdir("cool")
		assert.NoError(t, err)
		assertNewDir(t, fs, newDir, "cool")

		// cd down
		_, err = fs.Cd("cool")
		assert.NoError(t, err)
		newDir, err = fs.Mkdir("but")
		assert.NoError(t, err)
		assertNewDir(t, fs, newDir, "but")
		newFile, err = fs.Touch("just wait...")
		assert.NoError(t, err)
		assertNewFileExists(t, fs, newFile, "just wait...", []string{"but", "just wait..."})

		// remove!!
		_, err = fs.Cd("..") // you
		assert.NoError(t, err)

		_, err = fs.Cd("..") // root
		assert.NoError(t, err)

		fs.Rm("you")
		assert.NotEqual(t, prevVGUID, fs.CurrentDir.GetVGUID())
		assert.Equal(t, uint64(1), fs.CurrentDir.GetSize())
		assert.Len(t, fs.CurrentDir.Blocks, 1)
		_, err = fs.Cd("you")
		assert.Error(t, err)

		contents, err := fs.Ls()
		assert.NoError(t, err)
		assert.ElementsMatch(t, []string{"know"}, contents)
	})
}

func TestDualClient(t *testing.T) {
	fs := InitFileSystem(t, 3)
	if fs == nil {
		return
	}
	defer fs.Close()

	rootOtherClient, err := puddlestore.OpenConn(puddlestore.DEFAULT_ZK_ADDR)
	if err != nil {
		t.Errorf("issue starting second client: %v", err)
		return
	}

	t.Run("first client create file", func(t *testing.T) {
		_, err := fs.Touch("dog")
		assert.NoError(t, err)

		contents, err := fs.Ls()
		assert.NoError(t, err)
		if err == nil {
			assert.Equal(t, []string{"dog"}, contents)
		}

		data := []byte("cats are cool")
		file, err := fs.WriteTo("dog", data)
		assert.NoError(t, err)
		if err == nil {
			assert.Equal(t, uint64(len(data)), file.GetSize())
		}

		retData, err := fs.Cat("dog")
		assert.NoError(t, err)
		if err == nil {
			assert.Equal(t, string(data), retData)
		}
	})

	t.Run("second client read file", func(t *testing.T) {
		rootOtherClient, err = rootOtherClient.Reload()
		assert.NoError(t, err)
		if rootOtherClient != nil {
			child, _, found, err := rootOtherClient.GetChildByName("dog")
			assert.NoError(t, err)
			assert.True(t, found)
			if child != nil {
				dataFrom, err := child.AsFile().Read()
				assert.NoError(t, err)
				assert.Equal(t, "cats are cool", string(dataFrom))

				err = child.AsFile().Write([]byte("dogs are cool"))
				assert.NoError(t, err)
			}
		}
	})

	t.Run("first client read changes", func(t *testing.T) {
		retData, err := fs.Cat("dog")
		assert.NoError(t, err)
		if err == nil {
			assert.Equal(t, "dogs are cool", retData)
		}
	})
}

func TestRmOutFromUnder(t *testing.T) {
	fs := InitFileSystem(t, 3)
	if fs == nil {
		return
	}
	defer fs.Close()

	rootOtherClient, err := puddlestore.OpenConn(puddlestore.DEFAULT_ZK_ADDR)
	if err != nil {
		t.Errorf("issue starting second client: %v", err)
		return
	}

	t.Run("first client cd", func(t *testing.T) {
		_, err := fs.Mkdir("this will")
		assert.NoError(t, err)
		_, err = fs.Cd("this will")
		assert.NoError(t, err)

		_, err = fs.Mkdir("be here")
		assert.NoError(t, err)
		_, err = fs.Cd("be here")
		assert.NoError(t, err)

		_, err = fs.Mkdir("right?")
		assert.NoError(t, err)
		_, err = fs.Cd("right?")
		assert.NoError(t, err)
	})

	t.Run("second client remove dir", func(t *testing.T) {
		rootOtherClient, err = rootOtherClient.Reload()
		assert.NoError(t, err)
		if rootOtherClient != nil {
			parent, _, found, err := rootOtherClient.GetChildByName("this will")
			assert.NoError(t, err)
			assert.True(t, found)
			if parent != nil {
				child, _, found, err := parent.GetChildByName("be here")
				assert.NoError(t, err)
				assert.True(t, found)

				child.Remove(parent.AsDirectory())
				assert.NoError(t, err)
			}
		}
	})

	t.Run("first client get out", func(t *testing.T) {
		_, err := fs.Touch("file!")
		assert.Error(t, err)
		_, err = fs.Mkdir("dir!")
		assert.Error(t, err)
		_, err = fs.Cd("dir!")
		assert.Error(t, err)
		_, err = fs.Cd("..")
		assert.Error(t, err)

		_, err = fs.Cd("/")
		assert.NoError(t, err)
		_, err = fs.Cd("/this will")
		assert.NoError(t, err)
	})
}

func TestKillTapestryNode(t *testing.T) {
	puddlestore.RemoveRaftLogs()
	_, tcluster, err := puddlestore.StartPuddleStoreCluster(puddlestore.DEFAULT_ZK_ADDR, 10)
	assert.NoError(t, err)
	fs, err := NewFileSystem(puddlestore.DEFAULT_ZK_ADDR)
	if err != nil || fs == nil || fs.CurrentDir == nil {
		t.Errorf("%v", err)
		if !assert.NotNil(t, fs) {
			return
		}
		assert.NotNil(t, fs.CurrentDir)
		return
	}

	data := puddlestore.GetRandBytes(puddlestore.DBLOCKSIZE * 3)
	t.Run("make files", func(t *testing.T) {
		_, err := fs.Touch("cats")
		assert.NoError(t, err)

		_, err = fs.Touch("dogs")
		assert.NoError(t, err)
		_, err = fs.WriteTo("dogs", data)
		assert.NoError(t, err)

		_, err = fs.Touch("lolz")
		assert.NoError(t, err)
		_, err = fs.WriteTo("lolz", []byte("data"))
		assert.NoError(t, err)
	})

	t.Run("make dirs", func(t *testing.T) {
		_, err := fs.Mkdir("this will")
		assert.NoError(t, err)
		_, err = fs.Cd("this will")
		assert.NoError(t, err)

		_, err = fs.Mkdir("be here")
		assert.NoError(t, err)
		_, err = fs.Cd("be here")
		assert.NoError(t, err)

		_, err = fs.Mkdir("right?")
		assert.NoError(t, err)
		_, err = fs.Cd("right?")
		assert.NoError(t, err)
	})

	tcluster.Nodes[0].Kill()

	t.Run("check dirs", func(t *testing.T) {
		_, err = fs.Ls()
		assert.NoError(t, err)
		_, err = fs.Cd("..")
		assert.NoError(t, err)

		_, err = fs.Ls()
		assert.NoError(t, err)
		_, err = fs.Cd("..")
		assert.NoError(t, err)

		_, err = fs.Ls()
		assert.NoError(t, err)
		_, err = fs.Cd("..")
		assert.NoError(t, err)

		_, err = fs.Cd("this will")
		assert.NoError(t, err)

		_, err = fs.Cd("be here")
		assert.NoError(t, err)

		_, err = fs.Cd("right?")
		assert.NoError(t, err)

		_, err = fs.Cd("/")
		assert.NoError(t, err)
	})

	t.Run("check files", func(t *testing.T) {
		out, err := fs.Cat("cats")
		assert.NoError(t, err)
		if err == nil {
			assert.Equal(t, "", out)
		}

		out, err = fs.Cat("dogs")
		assert.NoError(t, err)
		if err == nil {
			assert.Equal(t, string(data), out)
		}

		out, err = fs.Cat("lolz")
		assert.NoError(t, err)
		if err == nil {
			assert.Equal(t, "data", out)
		}
	})
}
