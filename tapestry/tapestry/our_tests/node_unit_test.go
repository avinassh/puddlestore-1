package tapestry

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

// Detailed edge case test
func TestInsertWithoutDuplicates_SortListByCloseness(t *testing.T) {
	localNode, _ := start(ID{0, 0, 0, 2}, 0, "")
	neighbors := make([]RemoteNode, 0)

	neighbors = InsertWithoutDuplicates(neighbors, []RemoteNode{{ID{0, 0, 0, 0}, ""}})
	assert.Equal(t,
		[]RemoteNode{
			{ID{0, 0, 0, 0}, ""}},
		neighbors)

	neighbors = localNode.SortListByCloseness(neighbors)
	assert.Equal(t,
		[]RemoteNode{
			{ID{0, 0, 0, 0}, ""}},
		neighbors)

	neighbors = InsertWithoutDuplicates(neighbors, []RemoteNode{{ID{0, 0, 0, 0}, ""}})
	assert.Equal(t,
		[]RemoteNode{
			{ID{0, 0, 0, 0}, ""}},
		neighbors)

	neighbors = InsertWithoutDuplicates(neighbors, []RemoteNode{{ID{9, 12, 4, 15}, ""}})
	assert.ElementsMatch(t,
		[]RemoteNode{
			{ID{0, 0, 0, 0}, ""},
			{ID{9, 12, 4, 15}, ""}},
		neighbors)

	neighbors = InsertWithoutDuplicates(neighbors, []RemoteNode{{ID{9, 12, 4, 15}, ""}})
	assert.ElementsMatch(t,
		[]RemoteNode{
			{ID{0, 0, 0, 0}, ""},
			{ID{9, 12, 4, 15}, ""}},
		neighbors)

	neighbors = localNode.SortListByCloseness(neighbors)
	assert.Equal(t,
		[]RemoteNode{
			{ID{0, 0, 0, 0}, ""},
			{ID{9, 12, 4, 15}, ""}},
		neighbors)

	neighbors = InsertWithoutDuplicates(neighbors, []RemoteNode{{ID{3, 16, 5, 16}, ""}})
	assert.ElementsMatch(t,
		[]RemoteNode{
			{ID{0, 0, 0, 0}, ""},
			{ID{3, 16, 5, 16}, ""},
			{ID{9, 12, 4, 15}, ""}},
		neighbors)

	neighbors = InsertWithoutDuplicates(neighbors, []RemoteNode{{ID{3, 16, 5, 16}, ""}})
	assert.ElementsMatch(t,
		[]RemoteNode{
			{ID{0, 0, 0, 0}, ""},
			{ID{3, 16, 5, 16}, ""},
			{ID{9, 12, 4, 15}, ""}},
		neighbors)

	neighbors = localNode.SortListByCloseness(neighbors)
	assert.Equal(t,
		[]RemoteNode{
			{ID{0, 0, 0, 0}, ""},
			{ID{3, 16, 5, 16}, ""},
			{ID{9, 12, 4, 15}, ""}},
		neighbors)

	neighbors = InsertWithoutDuplicates(neighbors, []RemoteNode{{ID{15, 1, 3, 9}, ""}})
	assert.ElementsMatch(t,
		[]RemoteNode{
			{ID{0, 0, 0, 0}, ""},
			{ID{3, 16, 5, 16}, ""},
			{ID{9, 12, 4, 15}, ""},
			{ID{15, 1, 3, 9}, ""}},
		neighbors)

	neighbors = InsertWithoutDuplicates(neighbors, []RemoteNode{{ID{15, 1, 3, 9}, ""}})
	assert.ElementsMatch(t,
		[]RemoteNode{
			{ID{0, 0, 0, 0}, ""},
			{ID{3, 16, 5, 16}, ""},
			{ID{9, 12, 4, 15}, ""},
			{ID{15, 1, 3, 9}, ""}},
		neighbors)

	neighbors = localNode.SortListByCloseness(neighbors)
	assert.Equal(t,
		[]RemoteNode{
			{ID{0, 0, 0, 0}, ""},
			{ID{3, 16, 5, 16}, ""},
			{ID{9, 12, 4, 15}, ""},
			{ID{15, 1, 3, 9}, ""}},
		neighbors)

	// same dist
	id := ID{0, 0, 0, 4}
	assert.False(t, localNode.node.Id.Closer(id, ID{0, 0, 0, 0}))
	assert.False(t, localNode.node.Id.Closer(ID{0, 0, 0, 0}, id))
	neighbors = InsertWithoutDuplicates(neighbors, []RemoteNode{{id, ""}})
	assert.ElementsMatch(t,
		[]RemoteNode{
			{ID{0, 0, 0, 0}, ""},
			{id, ""},
			{ID{3, 16, 5, 16}, ""},
			{ID{9, 12, 4, 15}, ""},
			{ID{15, 1, 3, 9}, ""}},
		neighbors)

	neighbors = localNode.SortListByCloseness(neighbors)
	assert.Equal(t,
		[]RemoteNode{
			{ID{0, 0, 0, 0}, ""},
			{id, ""},
			{ID{3, 16, 5, 16}, ""},
			{ID{9, 12, 4, 15}, ""},
			{ID{15, 1, 3, 9}, ""}},
		neighbors)

	neighbors = InsertWithoutDuplicates(neighbors, []RemoteNode{{ID{0, 0, 0, 2}, ""}})
	assert.ElementsMatch(t,
		[]RemoteNode{
			{ID{0, 0, 0, 2}, ""},
			{ID{0, 0, 0, 0}, ""},
			{id, ""},
			{ID{3, 16, 5, 16}, ""},
			{ID{9, 12, 4, 15}, ""},
			{ID{15, 1, 3, 9}, ""}},
		neighbors)

	neighbors = localNode.SortListByCloseness(neighbors)
	assert.Equal(t,
		[]RemoteNode{
			{ID{0, 0, 0, 2}, ""},
			{ID{0, 0, 0, 0}, ""},
			{id, ""},
			{ID{3, 16, 5, 16}, ""},
			{ID{9, 12, 4, 15}, ""},
			{ID{15, 1, 3, 9}, ""}},
		neighbors)
}

func createRandRemoteNodes(size int) (remNodes []RemoteNode) {
	remNodes = make([]RemoteNode, size)
	for i := 0; i < size; i++ {
		id := RandomID()
		remNodes[i] = RemoteNode{id, fmt.Sprintf("%s:1234", id.String())}
	}
	return
}

func assertSortedDuplicateFreeNodeList(t *testing.T, remNodes []RemoteNode, localNode *Node) {
	for i := 0; i < len(remNodes)-1; i++ {
		// sorted
		assert.True(t, localNode.node.Id.Closer(remNodes[i].Id, remNodes[i+1].Id))
		// no duplicates
		for j, node := range remNodes {
			if i != j {
				assert.NotEqual(t, node, remNodes[i])
			}
		}
	}
}

// Tests the high-level properties of the duplicate-removing and sorting helper function
func TestAppendNeighborCandidates(t *testing.T) {
	localNode, _ := start(RandomID(), 0, "") // ID{0, 0, 0, 0}
	if localNode == nil {
		return
	}

	remNodes := createRandRemoteNodes(200)
	neighbors := make([]RemoteNode, 0)
	for i, node := range remNodes {
		neighbors = InsertWithoutDuplicates(neighbors, []RemoteNode{node})
		neighbors = localNode.SortListByCloseness(neighbors)
		assertSortedDuplicateFreeNodeList(t, neighbors, localNode)
		assert.Equal(t, i+1, len(neighbors))
	}
}
