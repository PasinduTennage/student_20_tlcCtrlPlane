package gentree

import (
	"errors"
	"fmt"
	"go.dedis.ch/onet/v3"
	"go.dedis.ch/onet/v3/log"
	"go.dedis.ch/onet/v3/network"
	"strconv"
	"strings"
)

// LocalityNode represents a locality preserving node.
type LocalityNode struct {
	Name                string
	IP                  map[string]bool
	X                   float64
	Y                   float64
	Level               int
	ADist               []float64 // ADist[l] - minimum distance to level l
	PDist               []string  // pDist[l] - the node at level l whose distance from the crt Node isADist[l]
	Cluster             map[string]bool
	Bunch               map[string]bool
	OptimalCluster      map[string]bool
	OptimalBunch        map[string]bool
	Rings               []string
	NrOwnRings          int
	ServerIdentity      *network.ServerIdentity
	AvailablePortsStart int
	AvailablePortsEnd   int
	NextPort            int
}

// LocalityNodes is a list of LocalityNode
type LocalityNodes struct {
	All                   []*LocalityNode
	ServerIdentityToName  map[network.ServerIdentityID]string
}

// GetByIP gets the node by IP.
func (ns LocalityNodes) GetByIP(ip string) *LocalityNode {

	for _, n := range ns.All {
		if n.IP[ip] {
			return n
		}
	}
	return nil
}

func (ns LocalityNodes) GetByServerIdentityIP(ip string) *LocalityNode {

	for _, n := range ns.All {
		//if strings.Contains(n.ServerIdentity.String(), ip) {
		if n.IP[ip] {
			return n
		}
	}
	return nil
}

// GetByName gets the node by name.
func (ns LocalityNodes) GetByName(name string) *LocalityNode {
	nodeIdx := NodeNameToInt(name)

	//log.LLvl1("name here is", name)

	//log.LLvl1("ns length", len(ns.All), "nodeIdx", nodeIdx)
	if len(ns.All) < nodeIdx {
		//log.LLvl1("returning NOT fine")
		return nil
	}
	//log.LLvl1("returning fine", ns.All[nodeIdx])
	//log.LLvl1(ns.All)
	return ns.All[nodeIdx%len(ns.All)]
	return ns.All[nodeIdx]
}

// NameToServerIdentity gets the server identity by name.
func (ns LocalityNodes) NameToServerIdentity(name string) *network.ServerIdentity {
	node := ns.GetByName(name)

	if node != nil {

		if node.ServerIdentity == nil {
			log.Error("nil 1", node, node.ServerIdentity)
		}

		if node.ServerIdentity.Address == "" {
			log.Error("nil 2", node.ServerIdentity.Address)
		}

		return node.ServerIdentity
	}

	return nil
}

// GetServerIdentityToName gets the name by server identity.
func (ns LocalityNodes) GetServerIdentityToName(sid *network.ServerIdentity) string {
	return fmt.Sprintf("node_%s", sid.Description[4:])
}

func findTreeNode(tree *onet.Tree, target *onet.TreeNode) (*onet.TreeNode, error) {

	for _, node := range tree.List() {
		if node.ID.Equal(target.ID) {
			return node, nil
		}
	}
	return nil, errors.New("not found")
}

//Converts a Node to it's index
func NodeNameToInt(nodeName string) int {
	substr := strings.Trim(nodeName, "node")
	substr = strings.Trim(substr, "_")

	idx, err := strconv.Atoi(substr)
	if err != nil {
		panic(err.Error())
	}
	return idx
}

type ByServerIdentityAlphabetical []*network.ServerIdentity

func (a ByServerIdentityAlphabetical) Len() int {
	return len(a)
}

func (a ByServerIdentityAlphabetical) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}

func (a ByServerIdentityAlphabetical) Less(i, j int) bool {
	return a[i].String() < a[j].String()
}