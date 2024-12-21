package bhash

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math/big"
	"sort"
	"sync"
)

type Bignum struct {
	*big.Int
}

// NewBignum takes a string number and converts it into a Bignum instance
func NewBignum(n string) *Bignum {
	x1, ok := new(big.Int).SetString(n, 0)
	if !ok {
		fmt.Println("fail to convert")
		return nil
	}
	return &Bignum{x1}
}

type SortedKeys []*Bignum

func (x SortedKeys) Len() int {
	return len(x)
}

func (x SortedKeys) Less(i, j int) bool {
	return (x[i].Cmp(x[j].Int) < 0)
}

func (x SortedKeys) Swap(i, j int) {
	x[i], x[j] = x[j], x[i]
}

// sha256_sum calculates the SHA-256 checksum for the bytes array,
// and then converts it into a Bignum instance
func sha256_sum(s []byte) *Bignum {
	out := sha256.Sum256(s)
	// Convert to hex string
	hexstr := hex.EncodeToString(out[:])
	// Convert hex string to BigInt
	num, _ := new(big.Int).SetString(hexstr, 16)
	return &Bignum{num}
}

// Configuration for the ConsistentHashing
type Config struct {
	ReplicationFactor int
}

// ConsistentHash structure
type ConsistentHash struct {
	config         Config
	sortedHashKeys SortedKeys
	ring           map[string]string
	dataSet        map[string]bool
	mu             sync.Mutex
}

// Create new Consistent Hashing instance
func New(config Config) *ConsistentHash {
	c := &ConsistentHash{
		config:  config,
		ring:    make(map[string]string),
		dataSet: make(map[string]bool),
	}
	return c
}

// Create new Consistent Hashing instance with nodes
func NewWithNodes(nodes []string, config Config) *ConsistentHash {
	c := &ConsistentHash{
		config:  config,
		ring:    make(map[string]string),
		dataSet: make(map[string]bool),
	}
	for _, n := range nodes {
		c.Add(n)
	}
	return c
}

// Get a nearest object name from input object in consistent hashing ring
func (c *ConsistentHash) Get(key string) string {
	index := c.searchRingIndex(key)
	skey := c.sortedHashKeys[index]
	s := skey.String()
	node, found := c.ring[s]
	if !found {
		fmt.Println("cannot find value for key ", skey)
		return ""
	}
	return node
}

// Add the name of the node (string) to the ring
func (c *ConsistentHash) Add(nodename string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	names := c.getNodeKeys(nodename)
	for vname, hkey := range names {
		s := hkey.String()
		c.ring[s] = nodename
		c.dataSet[vname] = true
	}
	c.updateSortHashKeys()
}

// Delete the node (string) from the ring
func (c *ConsistentHash) Remove(nodename string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	names := c.getNodeKeys(nodename)
	for vname, hkey := range names {
		delete(c.ring, hkey.String())
		delete(c.dataSet, vname)
	}
	c.updateSortHashKeys()
}

// Get all the nodes from the ring
func (c *ConsistentHash) GetNodeNames() []string {
	var out []string
	for _, key := range c.sortedHashKeys {
		node, found := c.ring[key.String()]
		if found {
			out = append(out, node)
		}
	}
	return out
}

// Based on the number of replicas, this will return an array of node names
func (c *ConsistentHash) getNodeKeys(nodename string) map[string]*Bignum {
	out := make(map[string]*Bignum)
	for i := 0; i < c.config.ReplicationFactor; i++ {
		s := fmt.Sprintf("%s:%d", nodename, i)
		h := c.hashKey(s)
		out[s] = h
	}
	return out
}

// The node replica with a hash value nearest but not less than that of the given
// name is returned. If the hash of the given name is greater than the greatest
// hash, returns the lowest hashed node.
func (c *ConsistentHash) searchRingIndex(obj string) int {
	count := len(c.sortedHashKeys)
	targetKey := c.hashKey(obj)

	fn := func(i int) bool {
		x := c.sortedHashKeys[i]
		y := targetKey
		return (x.Cmp(y.Int) > 0)
	}

	targetIndex := sort.Search(count, fn)
	if targetIndex >= count {
		targetIndex = 0
	}
	return targetIndex
}

func (c *ConsistentHash) updateSortHashKeys() {
	c.sortedHashKeys = nil
	for nodename := range c.dataSet {
		key := c.hashKey(nodename)
		c.sortedHashKeys = append(c.sortedHashKeys, key)
	}
	sort.Sort(c.sortedHashKeys)
}

// hashKey computes the hash using SHA-256 for the given object
func (c *ConsistentHash) hashKey(obj string) *Bignum {
	return sha256_sum([]byte(obj))
}
