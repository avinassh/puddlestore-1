/*
 *  Brown University, CS138, Spring 2018
 *
 *  Purpose: Defines BlobStore struct and provides get/put/delete methods for
 *  interacting with it.
 */

package tapestry

import (
	"sync"
)

// This is a utility class tacked on to the tapestry DOLR.  You should not need
// to use this directly.
type BlobStore struct {
	blobs map[string]Blob
	sync.RWMutex
}

type Blob struct {
	bytes []byte
	done  []chan bool
}

// Create a new blobstore
func NewBlobStore() *BlobStore {
	bs := new(BlobStore)
	bs.blobs = make(map[string]Blob)
	return bs
}

// Get bytes from the blobstore
func (bs *BlobStore) Get(key string) ([]byte, bool) {
	blob, exists := bs.blobs[key]
	if exists {
		return blob.bytes, true
	} else {
		return nil, false
	}
}

// Store bytes in the blobstore
func (bs *BlobStore) Put(key string, blob []byte, unregister []chan bool) {
	bs.Lock()
	defer bs.Unlock()

	// If a previous blob exists, delete it by de-registering on all done channels
	previous, exists := bs.blobs[key]
	if exists {
		for _, reg := range previous.done {
			reg <- true
		}
	}

	// Register the new one
	bs.blobs[key] = Blob{blob, unregister}
}

// Store bytes without deleting if the key already exists
func (bs *BlobStore) PutWithoutDelete(key string, blob []byte, unregister chan bool) {
	bs.Lock()
	defer bs.Unlock()

	previous, exists := bs.blobs[key]
	if exists {
		// update the given value of previous
		previous.done = append(previous.done, unregister)
	} else {
		// make a new slice and add it
		slice := make([]chan bool, 1)
		slice[0] = unregister
		bs.blobs[key] = Blob{blob, slice}
	}

}

// Remove the blob and unregister it
func (bs *BlobStore) Delete(key string) bool {
	bs.Lock()
	defer bs.Unlock()

	// If a previous blob exists, unregister it
	previous, exists := bs.blobs[key]
	if exists {
		for _, reg := range previous.done {
			reg <- true
		}
	}
	delete(bs.blobs, key)
	return exists
}

func (bs *BlobStore) DeleteAll() {
	bs.Lock()
	defer bs.Unlock()

	for key, value := range bs.blobs {
		for _, reg := range value.done {
			reg <- true
		}
		delete(bs.blobs, key)
	}
}
