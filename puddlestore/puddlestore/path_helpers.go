package puddlestore

import (
	"strings"
)

/*
	Helpers for handling paths. In our system, all paths are absolute and all start with a slash (/)
	representing the root. They never end in a slash.
*/

type Path struct {
	path []string
}

/*
	Generates a path object from the given string. Every path given to this is a unique path, i.e
	the first item listed is a child of the root directory.
	Any number of trailing or leading slashes are ignored
	/course/cs138/hello => [course, cs138, hello]
	//course/Cas//// => [course, cas]
*/
func NewPath(path string) *Path {
	// preprocessing
	path = strings.Trim(path, PATH_SEPERATOR)

	// convert to list
	p := Path{}
	if len(path) == 0 {
		p.path = []string{}
	} else {
		p.path = strings.Split(path, PATH_SEPERATOR)
		// remove any inbetween empty strings (resulting from multiple slashes)
		for i := len(p.path) - 1; i >= 0; i-- {
			if p.path[i] == "" {
				p.path = append(p.path[:i], p.path[i+1:]...)
			}
		}
	}
	return &p
}

// List splits the given string into a slice of its individual directories or files,
// from the top level to the bottom level.
func (p *Path) List() []string {
	return p.path
}

// String returns the unix representation of the current path.
func (p *Path) String() string {
	return PATH_SEPERATOR + strings.Join(p.path, PATH_SEPERATOR)
}

// Append appends the given file or directory name to the current path
func (p *Path) Append(name string) *Path {
	return &Path{append(p.path, strings.Trim(name, PATH_SEPERATOR))}
}

// Append merges the new path onto the end of this path
func (p *Path) Concat(childPath *Path) *Path {
	return &Path{append(p.path, childPath.path...)}
}

// utility to determine if this (absolute) path represents the "root"
func (p *Path) IsRoot() bool {
	return len(p.path) == 0
}

// Basename returns only the name of the last, minus its directory.
func (p *Path) Basename() string {
	_, filename := p.Split()
	return filename
}

// Dirname returns the directory of the path, which is equivalent to path if path refers to a directory.
func (p *Path) Dirname() string {
	dirPath, _ := p.Split()
	return dirPath.String()
}

// DirnamePath returns the Dirname as a Path object
func (p *Path) DirnamePath() *Path {
	dirPath, _ := p.Split()
	return &dirPath
}

/*
	Split splits the path into its directory and basename components (all path elements before the last, and the last),
	and returns both.
*/
func (p *Path) Split() (dirPath Path, basename string) {
	if len(p.path) >= 2 {
		return Path{p.path[:len(p.path)-1]}, p.path[len(p.path)-1]
	} else if len(p.path) == 1 {
		return Path{[]string{}}, p.path[0]
	} else {
		return Path{[]string{}}, ""
	}
}

// IsPath returns whether the given string may be a path (rather than simply a directory or file name)
func IsPath(str string) bool {
	return strings.Contains(strings.Trim(str, PATH_SEPERATOR), PATH_SEPERATOR)
}
