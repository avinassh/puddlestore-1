package tapestry

import (
	"testing"
	"time"
	"strconv"
	"fmt"
)

func GenerateNetwork(nodes int) ([]*Node, error) {
	aseed := int64(1)
	ts, err := MakeRandomTapestries(aseed, 1)
	if err != nil {
		Error.Printf("error generating tapestry network: %v", err.Error())
		return nil, err
	}
	return ts, nil
}

func benchmarkNode_Store(size int, b *testing.B) {

	aseed := int64(1)
	ts, err := MakeRandomTapestries(aseed, size)
	if err != nil {
		Error.Printf("error generating tapestry network: %v", err.Error())
		return
	}

	fmt.Printf("FINISHED SETTING UP NETWORK\n")
	time.Sleep(5*time.Second)

	b.ResetTimer()
	for i :=0; i < b.N; i++ {
		node := ts[random.Intn(len(ts))]
		key := strconv.Itoa(random.Int())
		err := node.Store(key, []byte{})
		if err != nil {
			Error.Printf("%v", err.Error())
		}
		time.Sleep(10 * time.Millisecond)
	}
	fmt.Printf("FINISHED FILLING NETWORK\n")
}

// 100	  15853303 ns/op NO SALTING
func BenchmarkNode_Store1(b *testing.B) {benchmarkNode_Store(1, b)}

// 100	  16902679 ns/op NO SALTING
func BenchmarkNode_Store10(b *testing.B) {benchmarkNode_Store(10, b)}

// 100	  17633298 ns/op NO SALTING
func BenchmarkNode_Store100(b *testing.B) {benchmarkNode_Store(100, b)}

// 30	  43063417 ns/op NO SALTING
func BenchmarkNode_Store500(b *testing.B) {benchmarkNode_Store(500, b)}

// 30	  36258866 ns/op NO SALTING
func BenchmarkNode_Store1000(b *testing.B) {benchmarkNode_Store(1000, b)}

// benchmarkNode_Get generates a random tapestry network of n nodes, fills it with
// twice the number of nodes of random keys. It then benchmarks the cost of a single lookup
func benchmarkNode_Get(size int, b *testing.B) {

	// makes network
	aseed := int64(1)
	ts, err := MakeRandomTapestries(aseed, size)
	if err != nil {
		Error.Printf("error generating tapestry network: %v", err.Error())
		return
	}

	fmt.Printf("FINISHED GENERATING NETWORK\n")
	time.Sleep(10*time.Second)

	// fills network with data
	data := GenerateData(aseed, size*3, ts)
	for _, d := range data {
		err := d.store.Store(d.key, []byte{})
		if err != nil {
			Error.Printf("store: %v", err)
			return
		}
		time.Sleep(10 * time.Millisecond)
	}

	fmt.Printf("FINISHED FILLING NETWORK\n")
	time.Sleep(10 * time.Second)

	// resets timer and benchmarks lookup
	b.ResetTimer()
	for i :=0; i < b.N; i++ {
		spec := data[random.Intn(len(data))]
		node := spec.lookup
		nodes, err := node.Lookup(spec.key)
		if err != nil {
			Error.Printf("lookup: %v", err)
		}
		if !hasnode(nodes, spec.store.node) {
			Error.Printf("incorrect result of lookup")
		}
		time.Sleep(10 * time.Millisecond)
	}
	fmt.Printf("FINISHED QUERYING NETWORK\n")
}

//100	  15649259 ns/op
func BenchmarkNode_Get1(b *testing.B) {benchmarkNode_Get(1, b)}

//100	  15981820 ns/op
func BenchmarkNode_Get10(b *testing.B) {benchmarkNode_Get(10, b)}

//100	  17985317 ns/op
func BenchmarkNode_Get100(b *testing.B) {benchmarkNode_Get(100, b)}

//100	  21035664 ns/op
func BenchmarkNode_Get500(b *testing.B) {benchmarkNode_Get(500, b)}

//
func BenchmarkNode_Get1000(b *testing.B) {benchmarkNode_Get(1000, b)}