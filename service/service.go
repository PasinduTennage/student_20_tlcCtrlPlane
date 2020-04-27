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
	"math/rand"
	"sync"
	"time"
)

//var Name = template.ServiceName

var unwitnessedMessageMsgID network.MessageTypeID
var witnessedMessageMsgID network.MessageTypeID
var acknowledgementMessageMsgID network.MessageTypeID
var catchUpMessageID network.MessageTypeID

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
	catchUpMessageID = network.RegisterMessage(&template.CatchUpMessage{})
}

// Service is our template-service
type Service struct {
	// We need to embed the ServiceProcessor, so that incoming messages
	// are correctly handled.
	*onet.ServiceProcessor
	roster  *onet.Roster
	storage *storage

	step     int
	stepLock *sync.Mutex

	majority int

	sentUnwitnessMessages     map[int]*template.UnwitnessedMessage
	sentUnwitnessMessagesLock *sync.Mutex

	recievedUnwitnessedMessages     map[int][]*template.UnwitnessedMessage
	recievedUnwitnessedMessagesLock *sync.Mutex

	sentAcknowledgementMessages     map[int][]*template.AcknowledgementMessage
	sentAcknowledgementMessagesLock *sync.Mutex

	recievedAcknowledgesMessages     map[int][]*template.AcknowledgementMessage
	recievedAcknowledgesMessagesLock *sync.Mutex

	sentThresholdWitnessedMessages     map[int]*template.WitnessedMessage
	sentThresholdWitnessedMessagesLock *sync.Mutex

	recievedThresholdwitnessedMessages     map[int][]*template.WitnessedMessage
	recievedThresholdwitnessedMessagesLock *sync.Mutex

	recievedAcksBool     map[int]bool
	recievedAcksBoolLock *sync.Mutex

	recievedWitnessedMessagesBool     map[int]bool
	recievedWitnessedMessagesBoolLock *sync.Mutex
}

// storageID reflects the data we're storing - we could store more
// than one structure.
var storageID = []byte("main")

// storage is used to save our data.
type storage struct {
	Count int
	sync.Mutex
}

func broadcastUnwitnessedMessage(memberNodes []*network.ServerIdentity, s *Service, message *template.UnwitnessedMessage) {
	for _, node := range memberNodes {
		e := s.SendRaw(node, message)
		if e != nil {
			panic(e)
		}
	}
}

func broadcastWitnessedMessage(memberNodes []*network.ServerIdentity, s *Service, message *template.WitnessedMessage) {
	for _, node := range memberNodes {
		e := s.SendRaw(node, message)
		if e != nil {
			panic(e)
		}
	}
}

func unicastAcknowledgementMessage(memberNode *network.ServerIdentity, s *Service, message *template.AcknowledgementMessage) {
	e := s.SendRaw(memberNode, message)
	if e != nil {
		panic(e)
	}

}

func unicastCatchUpMessage(memberNode *network.ServerIdentity, s *Service, message *template.CatchUpMessage) {
	e := s.SendRaw(memberNode, message)
	if e != nil {
		panic(e)
	}

}

func (s *Service) InitRequest(req *template.InitRequest) (*template.InitResponse, error) {
	log.Lvl1("here", s.ServerIdentity().String())

	rand.Seed(time.Now().UTC().UnixNano())

	s.roster = req.SsRoster
	//fmt.Printf("Roster is set for %s", s.ServerIdentity())

	memberNodes := s.roster.List

	s.majority = len(memberNodes) / 2 + 1

	//time.Sleep(10 * time.Second)
	//fmt.Printf("Initial Broadcast from %s \n", s.ServerIdentity().String())

	s.stepLock.Lock()
	s.sentUnwitnessMessagesLock.Lock()

	stepNow:= s.step


	unwitnessedMessage := &template.UnwitnessedMessage{Step: stepNow, Id: s.ServerIdentity()}
	broadcastUnwitnessedMessage(memberNodes, s, unwitnessedMessage)


	s.sentUnwitnessMessages[stepNow] = unwitnessedMessage // check syncMaps in go

	s.sentUnwitnessMessagesLock.Unlock()
	s.stepLock.Unlock()

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

		step:     0,
		stepLock: new(sync.Mutex),

		sentUnwitnessMessages:     make(map[int]*template.UnwitnessedMessage),
		sentUnwitnessMessagesLock: new(sync.Mutex),

		recievedUnwitnessedMessages:     make(map[int][]*template.UnwitnessedMessage),
		recievedUnwitnessedMessagesLock: new(sync.Mutex),

		sentAcknowledgementMessages:     make(map[int][]*template.AcknowledgementMessage),
		sentAcknowledgementMessagesLock: new(sync.Mutex),

		recievedAcknowledgesMessages:     make(map[int][]*template.AcknowledgementMessage),
		recievedAcknowledgesMessagesLock: new(sync.Mutex),

		sentThresholdWitnessedMessages:     make(map[int]*template.WitnessedMessage),
		sentThresholdWitnessedMessagesLock: new(sync.Mutex),

		recievedThresholdwitnessedMessages:     make(map[int][]*template.WitnessedMessage),
		recievedThresholdwitnessedMessagesLock: new(sync.Mutex),

		recievedAcksBool:     make(map[int]bool),
		recievedAcksBoolLock: new(sync.Mutex),

		recievedWitnessedMessagesBool:     make(map[int]bool),
		recievedWitnessedMessagesBoolLock: new(sync.Mutex),
	}
	if err := s.RegisterHandlers(s.Clock, s.Count, s.InitRequest); err != nil {
		return nil, errors.New("Couldn't register messages")
	}
	if err := s.tryLoad(); err != nil {
		log.Error(err)
		return nil, err
	}

	s.RegisterProcessorFunc(unwitnessedMessageMsgID, func(arg1 *network.Envelope) error {

		//r := rand.Intn(10000000)
		//time.Sleep(time.Duration(r) * time.Microsecond)

		//r := rand.Intn(1000)
		//if r % 97 == 0 {
		//	fmt.Printf("Process %s decided to drop an unwitnessed message \n")
		//	return nil
		//}

		// Parse message
		req, ok := arg1.Msg.(*template.UnwitnessedMessage)
		if !ok {
			log.Error(s.ServerIdentity(), "failed to cast to unwitnessed message")
			return nil
		}
		//fmt.Printf("Received step is %d from %s by %s \n", req.Step, req.Id.String(), s.ServerIdentity())

		s.stepLock.Lock()
		s.recievedUnwitnessedMessagesLock.Lock()
		s.sentAcknowledgementMessagesLock.Lock()
		s.recievedThresholdwitnessedMessagesLock.Lock()

		stepNow:= s.step

		if req.Step == stepNow {

			s.recievedUnwitnessedMessages[req.Step] = append(s.recievedUnwitnessedMessages[req.Step], req)
			newAck := &template.AcknowledgementMessage{Id: s.ServerIdentity(), UnwitnessedMessage: req}
			requesterIdentity := req.Id
			unicastAcknowledgementMessage(requesterIdentity, s, newAck)
			s.sentAcknowledgementMessages[req.Step] = append(s.sentAcknowledgementMessages[req.Step], newAck)

		} else if req.Step < stepNow {

			fmt.Printf("Sending a catch up message because %s's time step which is %d is greater than %s's time step which is %d \n", s.ServerIdentity(), stepNow, req.Id, req.Step)
			// send a catch up message
			catchUpThresholdwitnessedMessages:=     make(map[int]*template.ArrayWitnessedMessages)
			for i := req.Step; i < stepNow; i++ {

				recievedMessages:= s.recievedThresholdwitnessedMessages[i]
				newArrayWitnessedMessages:= &template.ArrayWitnessedMessages{Messages:recievedMessages}
				catchUpThresholdwitnessedMessages[i] = newArrayWitnessedMessages
			}

			newCatchUpMessage := &template.CatchUpMessage{Id: s.ServerIdentity(), Step:stepNow, RecievedThresholdwitnessedMessages: catchUpThresholdwitnessedMessages}
			unicastCatchUpMessage(req.Id, s, newCatchUpMessage)
		}

		s.recievedThresholdwitnessedMessagesLock.Unlock()
		s.sentAcknowledgementMessagesLock.Unlock()
		s.recievedUnwitnessedMessagesLock.Unlock()
		s.stepLock.Unlock()


		return nil

	})

	s.RegisterProcessorFunc(acknowledgementMessageMsgID, func(arg1 *network.Envelope) error {


		//r := rand.Intn(1000)
		//if r % 97 == 0 {
		//	fmt.Printf("Process %s decided to drop an acknowledge message \n")
		//	return nil
		//}
		//r := rand.Intn(10000000)
		//time.Sleep(time.Duration(r) * time.Microsecond)

		// Parse message
		req, ok := arg1.Msg.(*template.AcknowledgementMessage)
		if !ok {
			log.Error(s.ServerIdentity(), "failed to cast to acknowledgement message")
			return nil
		}
		// //fmt.Printf("Received acknowledgement from %s by %s \n", req.Id.String(), s.ServerIdentity())
		s.stepLock.Lock()
		s.recievedAcknowledgesMessagesLock.Lock()
		s.recievedAcksBoolLock.Lock()
		s.sentThresholdWitnessedMessagesLock.Lock()

		s.recievedAcknowledgesMessages[req.UnwitnessedMessage.Step] = append(s.recievedAcknowledgesMessages[req.UnwitnessedMessage.Step], req)
		stepNow:= s.step
		hasEnoughAcks := s.recievedAcksBool[stepNow]


		if !hasEnoughAcks {
			lenRecievedAcks := len(s.recievedAcknowledgesMessages[stepNow])
			if lenRecievedAcks >= s.majority {
				//fmt.Printf("%s Recieved a majority of Acks \n", s.ServerIdentity())
				s.recievedAcksBool[stepNow] = true
				newWitness := &template.WitnessedMessage{Step: stepNow, Id: s.ServerIdentity()}
				broadcastWitnessedMessage(s.roster.List, s, newWitness)
				s.sentThresholdWitnessedMessages[stepNow] = newWitness
			}
		}
		s.sentThresholdWitnessedMessagesLock.Unlock()
		s.recievedAcksBoolLock.Unlock()
		s.recievedAcknowledgesMessagesLock.Unlock()
		s.stepLock.Unlock()

		return nil
	})

	s.RegisterProcessorFunc(witnessedMessageMsgID, func(arg1 *network.Envelope) error {


		//r := rand.Intn(1000)
		//if r % 97 ==0 {
		//	fmt.Printf("Process %s decided to drop an witnessed message \n")
		//	return nil
		//}
		//r := rand.Intn(10000000)
		//time.Sleep(time.Duration(r) * time.Microsecond)

		// Parse message
		req, ok := arg1.Msg.(*template.WitnessedMessage)
		if !ok {
			log.Error(s.ServerIdentity(), "failed to cast to witnessed message")
			return nil
		}
		//fmt.Printf("Received threshold witnessed message from %s by %s \n", req.Id.String(), s.ServerIdentity())

		s.stepLock.Lock()
		s.sentUnwitnessMessagesLock.Lock()
		s.recievedThresholdwitnessedMessagesLock.Lock()
		s.recievedWitnessedMessagesBoolLock.Lock()


		stepNow:= s.step
		s.recievedThresholdwitnessedMessages[req.Step] = append(s.recievedThresholdwitnessedMessages[req.Step], req)
		hasRecievedWitnessedMessages := s.recievedWitnessedMessagesBool[stepNow]

		if !hasRecievedWitnessedMessages {

			lenThresholdWitnessedMessages := len(s.recievedThresholdwitnessedMessages[stepNow])

			if lenThresholdWitnessedMessages >= s.majority {

				s.recievedWitnessedMessagesBool[stepNow] = true

				s.step = s.step + 1
				stepNow = s.step

				fmt.Printf("%s's time step is %d \n", s.ServerIdentity(), stepNow)

				unwitnessedMessage := &template.UnwitnessedMessage{Step: stepNow, Id: s.ServerIdentity()}

				if s.roster == nil {
					fmt.Printf("%s's roster is nil \n", s.ServerIdentity())
					return nil
				}

				if stepNow > 1000 {
					return nil
				}

				value, ok := s.sentUnwitnessMessages[stepNow]

				if !ok{
					broadcastUnwitnessedMessage(s.roster.List, s, unwitnessedMessage)
					s.sentUnwitnessMessages[stepNow] = unwitnessedMessage
				} else {
					fmt.Printf("Unwitnessed message %s for step %d from %s is already sent; possible race condition \n", value,stepNow, s.ServerIdentity())
				}
			}

		}
		s.recievedWitnessedMessagesBoolLock.Unlock()
		s.recievedThresholdwitnessedMessagesLock.Unlock()
		s.sentUnwitnessMessagesLock.Unlock()
		s.stepLock.Unlock()

		return nil
	})
	s.RegisterProcessorFunc(catchUpMessageID, func(arg1 *network.Envelope) error {

		//r := rand.Intn(10000000)
		//time.Sleep(time.Duration(r) * time.Microsecond)

		// Parse message
		req, ok := arg1.Msg.(*template.CatchUpMessage)
		if !ok {
			log.Error(s.ServerIdentity(), "failed to cast to catch up message")
			return nil
		}
		//fmt.Printf("Received catch up message from %s \n", req.Id.String())

		s.stepLock.Lock()
		s.sentUnwitnessMessagesLock.Lock()
		s.recievedThresholdwitnessedMessagesLock.Lock()
		s.recievedWitnessedMessagesBoolLock.Lock()


		stepNow:= s.step

		if stepNow < req.Step{

			catchUpMap:= req.RecievedThresholdwitnessedMessages
			for i := stepNow; i < req.Step; i++ {
				s.recievedThresholdwitnessedMessages[i] = catchUpMap[i].Messages
				s.recievedWitnessedMessagesBool[i] = true
			}

			s.step = req.Step
			stepNow = s.step

			fmt.Printf("%s's time step is %d (catched up) \n", s.ServerIdentity(), stepNow)

			unwitnessedMessage := &template.UnwitnessedMessage{Step: stepNow, Id: s.ServerIdentity()}

			if s.roster == nil {
				fmt.Printf("%s's roster is nil \n", s.ServerIdentity())
				return nil
			}

			if stepNow > 1000 {
				return nil
			}

			value, ok := s.sentUnwitnessMessages[stepNow]

			if !ok{
				broadcastUnwitnessedMessage(s.roster.List, s, unwitnessedMessage)
				s.sentUnwitnessMessages[stepNow] = unwitnessedMessage
			} else {
				fmt.Printf("Unwitnessed message %s for step %d from %s is already sent; possible race condition \n", value,stepNow, s.ServerIdentity())
			}

		}

		s.recievedWitnessedMessagesBoolLock.Unlock()
		s.recievedThresholdwitnessedMessagesLock.Unlock()
		s.sentUnwitnessMessagesLock.Unlock()
		s.stepLock.Unlock()

		return nil
	})

	return s, nil
}
