package service

/*
The service.go defines what to do for each API-call. This part of the service
runs on the node.
*/

import (
	"errors"
	"fmt"
	"github.com/dedis/cothority_template"
	"github.com/dedis/cothority_template/protocol"
	"go.dedis.ch/onet/v3"
	"go.dedis.ch/onet/v3/log"
	"go.dedis.ch/onet/v3/network"
	"sync"
	"time"
)

//var Name = template.ServiceName

var unwitnessedMessageMsgID network.MessageTypeID
var witnessedMessageMsgID network.MessageTypeID
var acknowledgementMessageMsgID network.MessageTypeID

// Used for tests
var templateID onet.ServiceID

func init() {
	var err error
	templateID, err = onet.RegisterNewService(template.ServiceName, newService)
	log.ErrFatal(err)
	network.RegisterMessage(&storage{})
	unwitnessedMessageMsgID = network.RegisterMessage(&template.UnwitnessedMessage{})
	witnessedMessageMsgID = network.RegisterMessage(&template.WitnessedMessage{})
	acknowledgementMessageMsgID = network.RegisterMessage(&template.AcknowledgementMessage{})

}

// Service is our template-service
type Service struct {
	// We need to embed the ServiceProcessor, so that incoming messages
	// are correctly handled.
	*onet.ServiceProcessor
	roster  *onet.Roster
	storage *storage
}

// storageID reflects the data we're storing - we could store more
// than one structure.
var storageID = []byte("main")

// storage is used to save our data.
type storage struct {
	Count int
	sync.Mutex
}

func (s *Service) InitRequest(req *template.InitRequest) (*template.InitResponse, error) {
	log.Lvl1("here", s.ServerIdentity().String())
	s.roster = req.SsRoster
	memberNodes := s.roster.List

	for _, node := range memberNodes {
		fmt.Printf("Broadcast from %s \n", s.ServerIdentity().String())
		e := s.SendRaw(node, &template.UnwitnessedMessage{Step: 0, Id: node})
		if e != nil {
			panic(e)
		}
	}

	return &template.InitResponse{}, nil
}

// Clock starts a template-protocol and returns the run-time.
func (s *Service) Clock(req *template.Clock) (*template.ClockReply, error) {
	s.storage.Lock()
	s.storage.Count++
	s.storage.Unlock()
	s.save()
	tree := req.Roster.GenerateNaryTreeWithRoot(2, s.ServerIdentity())
	if tree == nil {
		return nil, errors.New("couldn't create tree")
	}
	pi, err := s.CreateProtocol(protocol.Name, tree)
	if err != nil {
		return nil, err
	}
	start := time.Now()
	pi.Start()
	resp := &template.ClockReply{
		Children: <-pi.(*protocol.TemplateProtocol).ChildCount,
	}
	resp.Time = time.Now().Sub(start).Seconds()
	return resp, nil
}

// Count returns the number of instantiations of the protocol.
func (s *Service) Count(req *template.Count) (*template.CountReply, error) {
	s.storage.Lock()
	defer s.storage.Unlock()
	return &template.CountReply{Count: s.storage.Count}, nil
}

// NewProtocol is called on all nodes of a Tree (except the root, since it is
// the one starting the protocol) so it's the Service that will be called to
// generate the PI on all others node.
// If you use CreateProtocolOnet, this will not be called, as the Onet will
// instantiate the protocol on its own. If you need more control at the
// instantiation of the protocol, use CreateProtocolService, and you can
// give some extra-configuration to your protocol in here.
func (s *Service) NewProtocol(tn *onet.TreeNodeInstance, conf *onet.GenericConfig) (onet.ProtocolInstance, error) {
	log.Lvl3("Not templated yet")
	return nil, nil
}

// saves all data.
func (s *Service) save() {
	s.storage.Lock()
	defer s.storage.Unlock()
	err := s.Save(storageID, s.storage)
	if err != nil {
		log.Error("Couldn't save data:", err)
	}
}

// Tries to load the configuration and updates the data in the service
// if it finds a valid config-file.
func (s *Service) tryLoad() error {
	s.storage = &storage{}
	msg, err := s.Load(storageID)
	if err != nil {
		return err
	}
	if msg == nil {
		return nil
	}
	var ok bool
	s.storage, ok = msg.(*storage)
	if !ok {
		return errors.New("Data of wrong type")
	}
	return nil
}

// newService receives the context that holds information about the node it's
// running on. Saving and loading can be done using the context. The data will
// be stored in memory for tests and simulations, and on disk for real deployments.
func newService(c *onet.Context) (onet.Service, error) {
	s := &Service{
		ServiceProcessor: onet.NewServiceProcessor(c),
	}
	if err := s.RegisterHandlers(s.Clock, s.Count, s.InitRequest); err != nil {
		return nil, errors.New("Couldn't register messages")
	}
	if err := s.tryLoad(); err != nil {
		log.Error(err)
		return nil, err
	}

	s.RegisterProcessorFunc(unwitnessedMessageMsgID, func(arg1 *network.Envelope) error {
		// Parse message
		req, ok := arg1.Msg.(*template.UnwitnessedMessage)
		if !ok {
			log.Error(s.ServerIdentity(), "failed to cast to unwitnessed message")
			return nil
		}

		reply := "Test Message"
		myName := s.ServerIdentity()
		fmt.Printf("Received step is %d from %s \n", req.Step, req.Id.String())
		log.Lvl3("sending", reply)
		requesterIdentity := req.Id
		e := s.SendRaw(requesterIdentity, &template.AcknowledgementMessage{Id: myName, UnwitnessedMessage: *req})
		if e != nil {
			panic(e)
		}

		return nil
	})

	return s, nil
}
