package main

import (
	"fmt"
	"github.com/BurntSushi/toml"
	"github.com/dedis/cothority_template"
	"go.dedis.ch/onet/v3"
	"go.dedis.ch/onet/v3/log"
	"strconv"
	"time"
)

/*
 * Defines the simulation for the service-template
 */

func init() {
	onet.SimulationRegister("TemplateService", NewSimulationService)
}

// SimulationService only holds the BFTree simulation
type SimulationService struct {
	onet.SimulationBFTree
}

// NewSimulationService returns the new simulation, where all fields are
// initialised using the config-file
func NewSimulationService(config string) (onet.Simulation, error) {
	es := &SimulationService{}
	_, err := toml.Decode(config, es)
	if err != nil {
		return nil, err
	}
	return es, nil
}

// Setup creates the tree used for that simulation
func (s *SimulationService) Setup(dir string, hosts []string) (
	*onet.SimulationConfig, error) {
	sc := &onet.SimulationConfig{}
	s.CreateRoster(sc, hosts, 2000)
	err := s.CreateTree(sc)
	if err != nil {
		return nil, err
	}
	return sc, nil
}

// Node can be used to initialize each node before it will be run
// by the server. Here we call the 'Node'-method of the
// SimulationBFTree structure which will load the roster- and the
// tree-structure to speed up the first round.
func (s *SimulationService) Node(config *onet.SimulationConfig) error {
	index, _ := config.Roster.Search(config.Server.ServerIdentity.ID)
	if index < 0 {
		log.Fatal("Didn't find this node in roster")
	}
	log.Lvl3("Initializing node-index", index)

	return s.SimulationBFTree.Node(config)

}

func (s *SimulationService) Simulation1(config *onet.SimulationConfig) error {
	numGenesis := 5
	nodes := make([]string, numGenesis)
	clients := make([]*template.Client, len(config.Roster.List))

	for i := 0; i < len(config.Roster.List); i++ {
		clients[i] = template.NewClient()
	}

	for i := 0; i < len(config.Roster.List); i++ {
		_, _ = clients[i].SendRosterRequest(config.Roster.List[i], config.Roster)
	}

	for i := 0; i < numGenesis; i++ {
		nodes[i] = strconv.Itoa(i)
	}

	for i := 0; i < len(config.Roster.List); i++ {
		_, _ = clients[i].SetGenesisSignersRequest(config.Roster.List[i], nodes)
	}
	fmt.Printf("Started Genesis CRUX, Round 0 \n")

	for i := 0; i < numGenesis; i++ {
		_, _ = clients[i].SendInitRequest(config.Roster.List[i])
	}

	time.Sleep(8 * time.Second)

	fmt.Printf("Started Round 1 \n")

	for i := 5; i < 7; i++ {
		_, _ = clients[i].SendJoinRequest(config.Roster.List[i])
	}

	time.Sleep(8 * time.Second)

	for i := 5; i < 7; i++ {
		nodes = append(nodes, strconv.Itoa(i))
	}

	for i := 7; i < len(config.Roster.List); i++ {
		_, _ = clients[i].SetGenesisSignersRequest(config.Roster.List[i], nodes)
	}

	time.Sleep(1 * time.Second)

	fmt.Printf("Started Round 2 \n")
	for i := 7; i < 9; i++ {
		_, _ = clients[i].SendJoinRequest(config.Roster.List[i])
	}
	time.Sleep(8 * time.Second)

	for i := 7; i < 9; i++ {
		nodes = append(nodes, strconv.Itoa(i))
	}

	for i := 9; i < len(config.Roster.List); i++ {
		_, _ = clients[i].SetGenesisSignersRequest(config.Roster.List[i], nodes)
	}

	time.Sleep(1 * time.Second)

	fmt.Printf("Started Round 3 \n")
	for i := 9; i < 11; i++ {
		_, _ = clients[i].SendJoinRequest(config.Roster.List[i])
	}
	time.Sleep(8 * time.Second)

	for i := 9; i < 11; i++ {
		nodes = append(nodes, strconv.Itoa(i))
	}

	for i := 11; i < len(config.Roster.List); i++ {
		_, _ = clients[i].SetGenesisSignersRequest(config.Roster.List[i], nodes)
	}
	time.Sleep(1 * time.Second)

	fmt.Printf("Started Round 4 \n")
	for i := 11; i < 13; i++ {
		_, _ = clients[i].SendJoinRequest(config.Roster.List[i])
	}
	time.Sleep(8 * time.Second)

	for i := 11; i < 13; i++ {
		nodes = append(nodes, strconv.Itoa(i))
	}

	for i := 13; i < len(config.Roster.List); i++ {
		_, _ = clients[i].SetGenesisSignersRequest(config.Roster.List[i], nodes)
	}

	time.Sleep(1 * time.Second)

	fmt.Printf("Started Round 5 \n")
	for i := 13; i < 15; i++ {
		_, _ = clients[i].SendJoinRequest(config.Roster.List[i])
	}
	time.Sleep(8 * time.Second)

	for i := 13; i < 15; i++ {
		nodes = append(nodes, strconv.Itoa(i))
	}

	for i := 15; i < len(config.Roster.List); i++ {
		_, _ = clients[i].SetGenesisSignersRequest(config.Roster.List[i], nodes)
	}

	time.Sleep(30 * time.Second)

	fmt.Printf("End of simulations\n")
	return nil
}

func (s *SimulationService) Simulation2(config *onet.SimulationConfig) error {
	numGenesis := 15
	nodes := make([]string, numGenesis)
	clients := make([]*template.Client, len(config.Roster.List))

	for i := 0; i < len(config.Roster.List); i++ {
		clients[i] = template.NewClient()
	}

	for i := 0; i < len(config.Roster.List); i++ {
		_, _ = clients[i].SendRosterRequest(config.Roster.List[i], config.Roster)
	}

	for i := 0; i < numGenesis; i++ {
		nodes[i] = strconv.Itoa(i)
	}

	for i := 0; i < len(config.Roster.List); i++ {
		_, _ = clients[i].SetGenesisSignersRequest(config.Roster.List[i], nodes)
	}
	fmt.Printf("Started Genesis CRUX, Round 0 \n")

	for i := 0; i < numGenesis; i++ {
		_, _ = clients[i].SendInitRequest(config.Roster.List[i])
	}

	time.Sleep(8 * time.Second)

	fmt.Printf("Started Round 1 \n")

	for i := 0; i < 2; i++ {
		_, _ = clients[i].SendActiveRequest(config.Roster.List[i])
	}

	time.Sleep(8 * time.Second)

	fmt.Printf("Started Round 2 \n")

	for i := 2; i < 4; i++ {
		_, _ = clients[i].SendActiveRequest(config.Roster.List[i])
	}

	time.Sleep(8 * time.Second)

	fmt.Printf("Started Round 3 \n")

	for i := 4; i < 6; i++ {
		_, _ = clients[i].SendActiveRequest(config.Roster.List[i])
	}

	time.Sleep(100 * time.Second)

	fmt.Printf("End of simulations\n")
	return nil
}

func (s *SimulationService) Simulation3(config *onet.SimulationConfig) error {
	numGenesis := 15
	nodes := make([]string, numGenesis)
	clients := make([]*template.Client, len(config.Roster.List))

	for i := 0; i < len(config.Roster.List); i++ {
		clients[i] = template.NewClient()
	}

	for i := 0; i < len(config.Roster.List); i++ {
		_, _ = clients[i].SendRosterRequest(config.Roster.List[i], config.Roster)
	}

	for i := 0; i < numGenesis; i++ {
		nodes[i] = strconv.Itoa(i)
	}

	for i := 0; i < len(config.Roster.List); i++ {
		_, _ = clients[i].SetGenesisSignersRequest(config.Roster.List[i], nodes)
	}
	fmt.Printf("Started Genesis CRUX, Round 0 \n")

	for i := 0; i < numGenesis; i++ {
		_, _ = clients[i].SendInitRequest(config.Roster.List[i])
	}

	time.Sleep(20 * time.Second)

	fmt.Printf("End of simulations\n")
	return nil
}

// Run is used on the destination machines and runs a number of
// rounds
func (s *SimulationService) Run(config *onet.SimulationConfig) error {
	s.Simulation3(config)
	return nil
}
