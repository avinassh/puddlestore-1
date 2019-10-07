package puddlestore

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewPath(t *testing.T) {
	assert.Equal(t, []string{}, NewPath("").path)
	assert.Equal(t, []string{}, NewPath("////").path)
	assert.Equal(t, []string{"cat"}, NewPath("cat").path)
	assert.Equal(t, []string{"cat"}, NewPath("/cat").path)
	assert.Equal(t, []string{"cat"}, NewPath("/cat/").path)
	assert.Equal(t, []string{"cat"}, NewPath("/cat///").path)
	assert.Equal(t, []string{"cat"}, NewPath("cat///////").path)
	assert.Equal(t, []string{"cat"}, NewPath("///////cat").path)

	assert.Equal(t, []string{"the", "cat"}, NewPath("/the////cat").path)
	assert.Equal(t, []string{"the", "cat"}, NewPath("////the///////cat////").path)
	assert.Equal(t, []string{"the", "dog", "is", "cat"}, NewPath("/the/dog/is////cat").path)
	assert.Equal(t, []string{"a", "a", "a", ",", "asdg;", "asdg", "asd"}, NewPath("a/a/a/////,/asdg;/asdg/asd").path)
}

func TestSplit(t *testing.T) {
	dirPath, basename := NewPath("").Split()
	assert.Equal(t, []string{}, dirPath.path)
	assert.Equal(t, "", basename)

	dirPath, basename = NewPath("cat").Split()
	assert.Equal(t, []string{}, dirPath.path)
	assert.Equal(t, "cat", basename)

	dirPath, basename = NewPath("lolz_are/").Split()
	assert.Equal(t, []string{}, dirPath.path)
	assert.Equal(t, "lolz_are", basename)

	dirPath, basename = NewPath("/lolz_are").Split()
	assert.Equal(t, []string{}, dirPath.path)
	assert.Equal(t, "lolz_are", basename)

	dirPath, basename = NewPath("cat/dog").Split()
	assert.Equal(t, []string{"cat"}, dirPath.path)
	assert.Equal(t, "dog", basename)

	dirPath, basename = NewPath("/cat/dog").Split()
	assert.Equal(t, []string{"cat"}, dirPath.path)
	assert.Equal(t, "dog", basename)

	dirPath, basename = NewPath("/cat/dog/").Split()
	assert.Equal(t, []string{"cat"}, dirPath.path)
	assert.Equal(t, "dog", basename)

	dirPath, basename = NewPath("/the/quick/brown/fox").Split()
	assert.Equal(t, []string{"the", "quick", "brown"}, dirPath.path)
	assert.Equal(t, "fox", basename)

	dirPath, basename = NewPath("/the/quick/brown/fox/jumped/").Split()
	assert.Equal(t, []string{"the", "quick", "brown", "fox"}, dirPath.path)
	assert.Equal(t, "jumped", basename)
}
