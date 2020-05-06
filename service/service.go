package service

/*
The service.go defines what to do for each API-call. This part of the service
runs on the node.
*/

import (
	"encoding/json"
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

	maxTime int

	sentUnwitnessMessages     map[int]*template.UnwitnessedMessage
	sentUnwitnessMessagesLock *sync.Mutex

	recievedUnwitnessedMessages     map[int][]*template.UnwitnessedMessage
	recievedUnwitnessedMessagesLock *sync.Mutex

	recievedTempUnwitnessedMessages     map[int][]*template.UnwitnessedMessage
	recievedTempUnwitnessedMessagesLock *sync.Mutex

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

	defer s.stepLock.Unlock()
	s.stepLock.Lock()

	s.roster = req.SsRoster
	//fmt.Printf("Roster is set for %s", s.ServerIdentity())

	memberNodes := s.roster.List

	s.majority = len(memberNodes)/2 + 1

	s.maxTime = 1000

	stepNow := s.step

	unwitnessedMessage := &template.UnwitnessedMessage{Step: stepNow, Id: s.ServerIdentity()}
	broadcastUnwitnessedMessage(memberNodes, s, unwitnessedMessage)

	s.sentUnwitnessMessages[stepNow] = unwitnessedMessage // check syncMaps in go

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

		recievedTempUnwitnessedMessages:     make(map[int][]*template.UnwitnessedMessage),
		recievedTempUnwitnessedMessagesLock: new(sync.Mutex),

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
		return nil, errors.New("couldn't register messages")
	}
	if err := s.tryLoad(); err != nil {
		log.Error(err)
		return nil, err
	}

	s.RegisterProcessorFunc(unwitnessedMessageMsgID, func(arg1 *network.Envelope) error {

		defer s.stepLock.Unlock()
		s.stepLock.Lock()
		// Parse message
		req, ok := arg1.Msg.(*template.UnwitnessedMessage)
		if !ok {
			log.Error(s.ServerIdentity(), "failed to cast to unwitnessed message")
			return nil
		}

		//fmt.Printf("%s at %d received unwitnessed from %s with step %d \n", s.ServerIdentity(), s.step, req.Id, req.Step)

		stepNow := s.step

		if req.Step == stepNow {

			s.recievedUnwitnessedMessages[req.Step] = append(s.recievedUnwitnessedMessages[req.Step], req)
			newAck := &template.AcknowledgementMessage{Id: s.ServerIdentity(), UnwitnessedMessage: req}
			requesterIdentity := req.Id
			unicastAcknowledgementMessage(requesterIdentity, s, newAck)
			s.sentAcknowledgementMessages[req.Step] = append(s.sentAcknowledgementMessages[req.Step], newAck)
			//fmt.Printf("%s at %d sent ack to %s with step %d \n", s.ServerIdentity(), s.step, req.Id, newAck.UnwitnessedMessage.Step)

		} else if req.Step < stepNow {

			s.recievedUnwitnessedMessages[req.Step] = append(s.recievedUnwitnessedMessages[req.Step], req)
			newAck := &template.AcknowledgementMessage{Id: s.ServerIdentity(), UnwitnessedMessage: req}
			requesterIdentity := req.Id
			unicastAcknowledgementMessage(requesterIdentity, s, newAck)
			s.sentAcknowledgementMessages[req.Step] = append(s.sentAcknowledgementMessages[req.Step], newAck)
			//fmt.Printf("%s at %d sent ack to %s with step %d \n", s.ServerIdentity(), s.step, req.Id, newAck.UnwitnessedMessage.Step)

			catchUpThresholdwitnessedMessages := make(map[int]*template.ArrayWitnessedMessages)
			for i := req.Step; i < stepNow; i++ {

				recievedMessages := s.recievedThresholdwitnessedMessages[i]
				newArrayWitnessedMessages := &template.ArrayWitnessedMessages{Messages: recievedMessages}
				catchUpThresholdwitnessedMessages[i] = newArrayWitnessedMessages
			}

			newCatchUpMessage := &template.CatchUpMessage{Id: s.ServerIdentity(), Step: stepNow, RecievedThresholdwitnessedMessages: catchUpThresholdwitnessedMessages}
			unicastCatchUpMessage(req.Id, s, newCatchUpMessage)
			//fmt.Printf("%s at %d sent catchup to %s with step %d \n", s.ServerIdentity(), s.step, req.Id, stepNow)

		} else if req.Step > stepNow {
			// save the unwitnessed message and later send ack
			s.recievedTempUnwitnessedMessages[req.Step] = append(s.recievedTempUnwitnessedMessages[req.Step], req)
		}

		return nil

	})

	s.RegisterProcessorFunc(acknowledgementMessageMsgID, func(arg1 *network.Envelope) error {

		defer s.stepLock.Unlock()
		s.stepLock.Lock()

		// Parse message
		req, ok := arg1.Msg.(*template.AcknowledgementMessage)
		if !ok {
			log.Error(s.ServerIdentity(), "failed to cast to acknowledgement message")
			return nil
		}

		//fmt.Printf("%s at %d received ack from %s with step %d \n", s.ServerIdentity(), s.step, req.Id, req.UnwitnessedMessage.Step)

		s.recievedAcknowledgesMessages[req.UnwitnessedMessage.Step] = append(s.recievedAcknowledgesMessages[req.UnwitnessedMessage.Step], req)
		stepNow := s.step
		hasEnoughAcks := s.recievedAcksBool[stepNow]

		if !hasEnoughAcks {
			// check whether this process has received a majority of acks from a majority of nodes

			lenRecievedAcks := len(s.recievedAcknowledgesMessages[stepNow])

			if lenRecievedAcks >= s.majority {

				// check if they are from distinct processes, no duplicates
				nodes := make([]*network.ServerIdentity, 0)
				for _, ack := range s.recievedAcknowledgesMessages[stepNow] {
					ackId:= ack.Id
					exists:= false
					for _, num := range nodes {
						numJson, _ := json.Marshal(num)
						ackIdJson, _ := json.Marshal(ackId)

						if string(numJson) == string (ackIdJson) {
							exists = true
							break
						}
					}
					if !exists{
						nodes = append(nodes, ackId)
					}
				}
				if len(nodes) >=s.majority{
					//fmt.Printf("%s Recieved a majority of Acks \n", s.ServerIdentity())
					s.recievedAcksBool[stepNow] = true
					newWitness := &template.WitnessedMessage{Step: stepNow, Id: s.ServerIdentity()}
					broadcastWitnessedMessage(s.roster.List, s, newWitness)
					s.sentThresholdWitnessedMessages[stepNow] = newWitness
					//fmt.Printf("%s at %d broadcast witnessed with step %d \n", s.ServerIdentity(), s.step, newWitness.Step)
				}

			}
		}

		return nil
	})

	s.RegisterProcessorFunc(witnessedMessageMsgID, func(arg1 *network.Envelope) error {

		defer s.stepLock.Unlock()
		s.stepLock.Lock()

		// Parse message
		req, ok := arg1.Msg.(*template.WitnessedMessage)
		if !ok {
			log.Error(s.ServerIdentity(), "failed to cast to witnessed message")
			return nil
		}


		//fmt.Printf("%s at %d received witnessed from %s with step %d \n", s.ServerIdentity(), s.step, req.Id, req.Step)

		stepNow := s.step
		s.recievedThresholdwitnessedMessages[req.Step] = append(s.recievedThresholdwitnessedMessages[req.Step], req)

		hasRecievedWitnessedMessages := s.recievedWitnessedMessagesBool[stepNow]

		if !hasRecievedWitnessedMessages {

			lenThresholdWitnessedMessages := len(s.recievedThresholdwitnessedMessages[stepNow])

			if lenThresholdWitnessedMessages >= s.majority {


				nodes := make([]*network.ServerIdentity, 0)
				for _, twm := range s.recievedThresholdwitnessedMessages[stepNow] {
					twmId:= twm.Id
					exists:= false
					for _, num := range nodes {
						numJson, _ := json.Marshal(num)
						twmIdJson, _ := json.Marshal(twmId)

						if string(numJson) == string (twmIdJson) {
							exists = true
							break
						}

					}
					if !exists{
						nodes = append(nodes, twmId)
					}
				}

				if len(nodes) >=s.majority{
					s.recievedWitnessedMessagesBool[stepNow] = true

					s.step = s.step + 1
					stepNow = s.step

					fmt.Printf("%s increased time step to %d \n", s.ServerIdentity(), stepNow)

					unwitnessedMessage := &template.UnwitnessedMessage{Step: stepNow, Id: s.ServerIdentity()}

					if s.roster == nil {
						fmt.Printf("%s's roster is nil \n", s.ServerIdentity())
						return nil
					}

					if stepNow > s.maxTime {
						return nil
					}

					value, ok := s.sentUnwitnessMessages[stepNow]

					if !ok {
						broadcastUnwitnessedMessage(s.roster.List, s, unwitnessedMessage)
						s.sentUnwitnessMessages[stepNow] = unwitnessedMessage
						//fmt.Printf("%s at %d broadcast unwitnessed with step %d \n", s.ServerIdentity(), s.step, s.step)
					} else {
						fmt.Printf("Unwitnessed message %s for step %d from %s is already sent; possible race condition \n", value, stepNow, s.ServerIdentity())
					}

					// check if there are unwitnessed messages to which this process didn't send and ack previously
					unAckedUnwitnessedMessages:= s.recievedTempUnwitnessedMessages[stepNow]
					for _, uauwm := range unAckedUnwitnessedMessages {
						s.recievedUnwitnessedMessages[uauwm.Step] = append(s.recievedUnwitnessedMessages[uauwm.Step], uauwm)
						newAck := &template.AcknowledgementMessage{Id: s.ServerIdentity(), UnwitnessedMessage: uauwm}
						requesterIdentity := uauwm.Id
						unicastAcknowledgementMessage(requesterIdentity, s, newAck)
						s.sentAcknowledgementMessages[uauwm.Step] = append(s.sentAcknowledgementMessages[uauwm.Step], newAck)
						//fmt.Printf("%s at %d sent ack to %s with step %d \n", s.ServerIdentity(), s.step, requesterIdentity, newAck.UnwitnessedMessage.Step)
					}

				}

			}

		}

		return nil
	})
	s.RegisterProcessorFunc(catchUpMessageID, func(arg1 *network.Envelope) error {

		defer s.stepLock.Unlock()
		s.stepLock.Lock()

		// Parse message
		req, ok := arg1.Msg.(*template.CatchUpMessage)
		if !ok {
			log.Error(s.ServerIdentity(), "failed to cast to catch up message")
			return nil
		}


		//fmt.Printf("%s at %d received catchup from %s with step %d \n", s.ServerIdentity(), s.step, req.Id, req.Step)

		stepNow := s.step

		if stepNow < req.Step {

			catchUpMap := req.RecievedThresholdwitnessedMessages
			for i := stepNow; i < req.Step; i++ {
				s.recievedThresholdwitnessedMessages[i] = catchUpMap[i].Messages
				s.recievedWitnessedMessagesBool[i] = true
				s.step = i+1
				stepNow = s.step

				// check if there are unwitnessed messages to which this process didn't send and ack previously
				unAckedUnwitnessedMessages:= s.recievedTempUnwitnessedMessages[stepNow]
				for _, uauwm := range unAckedUnwitnessedMessages {
					s.recievedUnwitnessedMessages[uauwm.Step] = append(s.recievedUnwitnessedMessages[uauwm.Step], uauwm)
					newAck := &template.AcknowledgementMessage{Id: s.ServerIdentity(), UnwitnessedMessage: uauwm}
					requesterIdentity := uauwm.Id
					unicastAcknowledgementMessage(requesterIdentity, s, newAck)
					s.sentAcknowledgementMessages[uauwm.Step] = append(s.sentAcknowledgementMessages[uauwm.Step], newAck)
					//fmt.Printf("%s at %d sent ack to %s with step %d \n", s.ServerIdentity(), s.step, requesterIdentity, newAck.UnwitnessedMessage.Step)
				}
			}

			s.step = req.Step
			stepNow = s.step

			fmt.Printf("%s' increased time step to %d  with catched up \n", s.ServerIdentity(), stepNow)

			unwitnessedMessage := &template.UnwitnessedMessage{Step: stepNow, Id: s.ServerIdentity()}

			if s.roster == nil {
				fmt.Printf("%s's roster is nil \n", s.ServerIdentity())
				return nil
			}

			if stepNow > s.maxTime {
				return nil
			}

			value, ok := s.sentUnwitnessMessages[stepNow]

			if !ok {
				broadcastUnwitnessedMessage(s.roster.List, s, unwitnessedMessage)
				s.sentUnwitnessMessages[stepNow] = unwitnessedMessage
				//fmt.Printf("%s at %d broadcast unwitnessed with step %d \n", s.ServerIdentity(), s.step, s.step)
			} else {
				fmt.Printf("Unwitnessed message %s for step %d from %s is already sent; possible race condition \n", value, stepNow, s.ServerIdentity())
			}

		}

		return nil
	})

	return s, nil
}
