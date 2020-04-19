package main

import (
	"github.com/BurntSushi/toml"
	"github.com/dedis/cothority_template"
	"github.com/dedis/cothority_template/gentree"
	"github.com/dedis/cothority_template/service"
	"go.dedis.ch/onet/v3"
	"go.dedis.ch/onet/v3/log"
	"go.dedis.ch/onet/v3/network"
	"go.dedis.ch/onet/v3/simul/monitor"
	"strings"
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
	Nodes gentree.LocalityNodes
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

func (s *SimulationService) InitializeMaps(config *onet.SimulationConfig, isLocalTest bool) map[*network.ServerIdentity]string {

	s.Nodes.ServerIdentityToName = make(map[network.ServerIdentityID]string)
	ServerIdentityToName := make(map[*network.ServerIdentity]string)

	nextPortsAvailable := make(map[string]int)
	portIncrement := 1000

	// get machines

	for _, node := range config.Tree.List() {
		machineAddr := strings.Split(strings.Split(node.ServerIdentity.Address.String(), "//")[1], ":")[0]
		//log.LLvl1("machineaddr", machineAddr)
		log.Lvl2("node addr", node.ServerIdentity.Address.String())
		nextPortsAvailable[machineAddr] = 20000
	}

	if isLocalTest {

		for _, treeNode := range config.Tree.List() {
			for i := range s.Nodes.All {

				machineAddr := strings.Split(strings.Split(treeNode.ServerIdentity.Address.String(), "//")[1], ":")[0]
				if !s.Nodes.All[i].IP[machineAddr] {
					continue
				}

				if s.Nodes.All[i].ServerIdentity != nil {
					// current node already has stuff assigned to it, get the next free one
					continue
				}

				if treeNode.ServerIdentity != nil && treeNode.ServerIdentity.Address == "" {
					log.Error("nil 132132", s.Nodes.All[i].Name)
				}

				s.Nodes.All[i].ServerIdentity = treeNode.ServerIdentity
				s.Nodes.All[i].ServerIdentity.Address = treeNode.ServerIdentity.Address

				// set reserved ports
				s.Nodes.All[i].AvailablePortsStart = nextPortsAvailable[machineAddr]
				s.Nodes.All[i].AvailablePortsEnd = s.Nodes.All[i].AvailablePortsStart + portIncrement
				// fot all IP addresses of the machine set the ports!

				for k, v := range s.Nodes.All[i].IP {
					if v {
						nextPortsAvailable[k] = s.Nodes.All[i].AvailablePortsEnd
					}
				}

				s.Nodes.All[i].NextPort = s.Nodes.All[i].AvailablePortsStart
				// set names
				s.Nodes.ServerIdentityToName[treeNode.ServerIdentity.ID] = s.Nodes.All[i].Name
				ServerIdentityToName[treeNode.ServerIdentity] = s.Nodes.All[i].Name

				log.Lvl1("associating", treeNode.ServerIdentity.String(), "to", s.Nodes.All[i].Name, "ports", s.Nodes.All[i].AvailablePortsStart, s.Nodes.All[i].AvailablePortsEnd, s.Nodes.All[i].ServerIdentity.Address)

				break
			}

		}
	} else {
		for _, treeNode := range config.Tree.List() {
			serverIP := treeNode.ServerIdentity.Address.Host()
			node := s.Nodes.GetByIP(serverIP)
			node.ServerIdentity = treeNode.ServerIdentity
			s.Nodes.ServerIdentityToName[treeNode.ServerIdentity.ID] = node.Name
			ServerIdentityToName[treeNode.ServerIdentity] = node.Name
			log.Lvl1("associating", treeNode.ServerIdentity.String(), "to", node.Name)
		}
	}

	return ServerIdentityToName
}

// Node can be used to initialize each node before it will be run
// by the server. Here we call the 'Node'-method of the
// SimulationBFTree structure which will load the roster- and the
// tree-structure to speed up the first round.
func (s *SimulationService) Node(config *onet.SimulationConfig) error {
	//index, _ := config.Roster.Search(config.Server.ServerIdentity.ID)
	//if index < 0 {
	//	log.Fatal("Didn't find this node in roster")
	//}
	//log.Lvl3("Initializing node-index", index)
	//return s.SimulationBFTree.Node(config)
	index, _ := config.Roster.Search(config.Server.ServerIdentity.ID)
	if index < 0 {
		log.Fatal("Didn't find this node in roster")
	}
	log.Lvl3("Initializing node-index", index)

	/* start */
	mymap := s.InitializeMaps(config, true)
	serviceReq := &service.InitRequest{
		Nodes:                s.Nodes.All,
		ServerIdentityToName: mymap,
		NrOps:                0,
		OpIdxStart:           2 * s.Hosts,
		Roster:               config.Roster,
	}
	myService := config.GetService(service.Name).(*service.Service)
	_, err := myService.InitRequest(serviceReq)
	if err != nil {
		return err
	}

	return s.SimulationBFTree.Node(config)
}

// Run is used on the destination machines and runs a number of
// rounds
func (s *SimulationService) Run(config *onet.SimulationConfig) error {
	size := config.Tree.Size()
	log.Lvl2("Size is:", size, "rounds:", s.Rounds)
	c := template.NewClient()
	for round := 0; round < 1; round++ {
		log.Lvl1("Starting round", round)
		round := monitor.NewTimeMeasure("round")
		resp, err := c.Clock(config.Roster)
		log.ErrFatal(err)
		if resp.Time <= 0 {
			log.Fatal("0 time elapsed")
		}
		round.Record()
	}
	return nil
}
