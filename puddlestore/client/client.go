package client

import (
	"errors"
	"fmt"
	"io/ioutil"

	"strings"

	ps "github.com/brown-csci1380/s18-mcisler-vmathur2/puddlestore/puddlestore"
)

/************************************************************************************/
// FileSystemState helpers to manage a notion of position within a PuddleStore filesystem
/************************************************************************************/

type FileSystemState struct {
	Root       *ps.INode     // the known Root of the puddlestore filesystem
	Path       *ps.Path      // the set of inode names to the current directory, including the current
	CurrentDir *ps.Directory // the current directory inode
}

func NewFileSystem(servers string) (*FileSystemState, error) {

	// Connect to Tapestry and get Root
	root, err := ps.OpenConn(servers)
	if err != nil {
		return nil, err
	}

	// create user state and start CLI repl
	state := FileSystemState{root, ps.NewPath("/"), root.AsDirectory()}
	return &state, nil
}

func (state *FileSystemState) Close() {
	state.CurrentDir.CloseConn()
}

/*
	Changes the current state directory to newDir, or to the parent directory if newDir is nil.
*/
func (state *FileSystemState) ChangeDir(newDir *ps.Directory) error {
	if newDir == nil {
		// go up if going to parent dir
		if state.Path.IsRoot() {
			return errors.New("no parent directory")
		}
		newPath := state.Path.DirnamePath()
		return state.ChangeDirByPath(newPath)
	} else {
		// go down if unknown dir
		newPath := state.Path.Append(newDir.GetName())
		return state.ChangeDirByPath(newPath)
	}
	return nil
}

func (state *FileSystemState) ChangeDirByPath(newPath *ps.Path) error {
	newCurDir, err := state.CurrentDir.OpenPath(newPath)
	if err != nil {
		return err
	}
	if newCurDir.IsFile() {
		return fmt.Errorf("%s is a file", newPath)
	}
	state.CurrentDir = newCurDir.AsDirectory()
	state.Path = newPath
	return nil
}

// Gets fresh copy of the current block by re-iterating from the filesystem root
func (state *FileSystemState) ReloadCurrentDir() error {
	curr, err := state.CurrentDir.Reload()
	if err != nil || curr == nil {
		curr, err = state.CurrentDir.OpenPath(state.Path)
		if err != nil {
			return errors.New("current directory no longer exists")
		}
		fmt.Println("warning: couldn't reload current directory")
	}
	state.CurrentDir = curr.AsDirectory()
	return nil
}

/************************************************************************************/
// CLI-like functions for interacting a PuddleStore filesystem
/************************************************************************************/

func (state *FileSystemState) Ls() ([]string, error) {

	// get fresh copy of the current block
	err := state.ReloadCurrentDir()
	if err != nil {
		return nil, err
	}

	// get contents of the block
	inodes, err := state.CurrentDir.Contents()
	if err != nil {
		return nil, err
	}
	contents := make([]string, len(inodes))
	for i, inode := range inodes {
		contents[i] = inode.GetName()
	}
	return contents, nil
}

/*
	cd changes the current directory of the fileSystem appropriately, and returns the new current dir.
	Returns nil error if success, and various errors otherwise.
*/
func (state *FileSystemState) Cd(dir string) (newDir *ps.Directory, err error) {
	if dir == ".." {
		// cd up
		err = state.ChangeDir(nil)
		if err != nil {
			return nil, err
		}
		return state.CurrentDir, nil
	} else if strings.HasPrefix(dir, ps.PATH_SEPERATOR) {
		// absolute path
		err = state.ChangeDirByPath(ps.NewPath(dir))
		if err != nil {
			return nil, err
		}
		return state.CurrentDir, nil
	} else {
		// relative path
		err = state.ChangeDirByPath(state.Path.Concat(ps.NewPath(dir)))
		if err != nil {
			return nil, err
		}
		return state.CurrentDir, nil

	}
}

func (state *FileSystemState) Mkdir(dir string) (*ps.Directory, error) {
	// get fresh copy of the current block
	err := state.ReloadCurrentDir()
	if err != nil {
		return nil, err
	}

	exists, err := state.Exists(dir)
	if err != nil {
		return nil, err
	}
	if !exists {
		return state.CurrentDir.CreateDirectory(dir)
	}
	return nil, fmt.Errorf("file or directory already exists with name %s", dir)
}

func (state *FileSystemState) Touch(file string) (*ps.File, error) {
	// get fresh copy of the current block
	err := state.ReloadCurrentDir()
	if err != nil {
		return nil, err
	}

	exists, err := state.Exists(file)
	if err != nil {
		return nil, err
	}
	if !exists {
		return state.CurrentDir.CreateFile(file)
	}
	return nil, fmt.Errorf("file or directory already exists with name %s", file)
}

func (state *FileSystemState) Rm(name string) error {
	err := state.ReloadCurrentDir()
	if err != nil {
		return err
	}

	inode, _, found, err := state.CurrentDir.GetChildByName(name)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("%s: no such file or directory", name)
	}
	return inode.Remove(state.CurrentDir)
}

func (state *FileSystemState) Cat(fname string) (string, error) {
	return state.CatN(fname, 0, 0)
}

func (state *FileSystemState) CatFrom(fname string, start uint64) (string, error) {
	return state.CatN(fname, start, 0)
}

func (state *FileSystemState) CatN(fname string, start, size uint64) (string, error) {

	// get fresh copy of the current block
	err := state.ReloadCurrentDir()
	if err != nil {
		return "", err
	}

	file, found, err := state.GetChildFile(fname)
	if err != nil {
		return "", err
	}
	if !found {
		return "", fmt.Errorf("%s: no such file", fname)
	}

	// read and dump bytes to screen
	bytes, err := file.ReadN(start, size)
	if err != nil {
		return "", err
	}

	return string(bytes), nil
}

/*
	WriteTo writes bytes to fname (a direct child of the current directory).
*/
func (state *FileSystemState) WriteTo(fname string, bytes []byte) (file *ps.File, err error) {
	file, found, err := state.GetChildFile(fname)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, fmt.Errorf("%s: no such file", fname)
	}
	return file, file.Write(bytes)
}

/*
	Scp
 */
func (state *FileSystemState) Scp(localAddr, remoteAddr string) (file *ps.File, err error) {

	// get fresh copy of the current block
	err = state.ReloadCurrentDir()
	if err != nil {
		return nil, err
	}

	// gets local bytes of file
	local, err := ioutil.ReadFile(localAddr)
	if err != nil {
		return nil, err
	}

	// create reference to INode of remote file
	file, err = state.CurrentDir.CreateFile(remoteAddr)
	if err != nil {
		return nil, err
	}

	// write to puddlestore
	return file, file.Write(local)
}

func (state *FileSystemState) Exists(name string) (bool, error) {
	_, _, found, err := state.CurrentDir.GetChildByName(name)
	return found, err
}

/*
	GetChildFile returns the file corresponding to filename that is a direct child of the current directory
*/
func (state *FileSystemState) GetChildFile(filename string) (file *ps.File, found bool, err error) {
	inode, _, found, err := state.CurrentDir.GetChildByName(filename)
	if err != nil {
		return nil, false, err
	}
	if found {
		if !inode.IsFile() {
			return nil, false, fmt.Errorf("%s: is a directory", filename)
		} else {
			return inode.AsFile(), true, nil
		}
	} else {
		return nil, false, nil
	}
}
