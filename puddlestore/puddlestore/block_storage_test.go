package puddlestore

import (
	"fmt"
	"testing"

	"math/rand"

	"github.com/stretchr/testify/assert"
)

const NUM_STORES = 200

func TestAddINodes(t *testing.T) {
	_, _, err := StartPuddleStoreCluster(DEFAULT_ZK_ADDR, 10)
	assert.NoError(t, err)
	conn, err := ZookeeperConnect(DEFAULT_ZK_ADDR)
	assert.NoError(t, err)
	if conn == nil {
		return
	}

	data := make([]*INode, NUM_STORES)
	t.Run("create", func(t *testing.T) {
		for i := 0; i < NUM_STORES; i++ {
			inode, err := createInode(conn, fmt.Sprintf("cat_%d", i), uint(rand.Intn(2)))
			assert.NoError(t, err)
			assert.NotNil(t, inode)
			inode.Blocks = []string(nil) // because glob does this
			data[i] = inode
		}
	})

	t.Run("read", func(t *testing.T) {
		for i := 0; i < NUM_STORES; i++ {
			assert.NotNil(t, data[i])
			if data[i] != nil {
				storedINodeData, err := fetchByAGUID(conn, data[i].GetAGUID())
				assert.NoError(t, err)
				inode, err := decodeInode(storedINodeData, conn)
				assert.NoError(t, err)
				assert.NotNil(t, inode)
				if inode != nil {
					assert.Equal(t, *data[i], *inode)
				}
			}
		}
	})
}

const NUM_BLOCKS = 1000

func TestAddTapestryBlocks(t *testing.T) {
	_, _, err := StartPuddleStoreCluster(DEFAULT_ZK_ADDR, 10)
	assert.NoError(t, err)
	conn, err := ZookeeperConnect(DEFAULT_ZK_ADDR)
	assert.NoError(t, err)
	if conn == nil {
		return
	}

	data := make([]*INode, NUM_BLOCKS)
	t.Run("create", func(t *testing.T) {
		for i := 0; i < NUM_BLOCKS; i++ {
			data[i] = &INode{
				conn,
				fmt.Sprintf("%d", i),
				fmt.Sprintf("%dV", i),
				fmt.Sprintf("cat_%d", i),
				uint(rand.Intn(2)),
				0,
				[]string(nil),
			}
			err := addObjectToTapestry(conn, data[i], data[i].GetVGUID())
			assert.NoError(t, err)
		}
	})

	t.Run("read", func(t *testing.T) {
		for i := 0; i < NUM_BLOCKS; i++ {
			storedINodeData, err := getObjectFromTapestry(conn, fmt.Sprintf("%dV", i))
			assert.NoError(t, err)
			inode, err := decodeInode(storedINodeData, conn)
			assert.NoError(t, err)
			assert.NotNil(t, inode)
			if inode != nil {
				assert.Equal(t, *data[i], *inode)
			}
		}
	})
}
