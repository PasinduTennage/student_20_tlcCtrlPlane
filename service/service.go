package service

import (
	"errors"
	"fmt"
	"github.com/dedis/cothority_template"
	"go.dedis.ch/onet/v3"
	"go.dedis.ch/onet/v3/log"
	"go.dedis.ch/onet/v3/network"
	"math/rand"
	"strconv"
	"sync"
	"time"
)

var unwitnessedMessageMsgID network.MessageTypeID
var witnessedMessageMsgID network.MessageTypeID
var acknowledgementMessageMsgID network.MessageTypeID
var nodeJoinRequestMessageMsgID network.MessageTypeID
var nodeJoinResponseMessageMsgID network.MessageTypeID
var nodeJoinConfirmationMessageMsgID network.MessageTypeID
var nodeJoinAdmissionCommitteeMessageMsgID network.MessageTypeID

var templateID onet.ServiceID

func init() {
	var err error
	templateID, err = onet.RegisterNewService(template.ServiceName, newService)
	log.ErrFatal(err)
	network.RegisterMessage(&storage{})
	unwitnessedMessageMsgID = network.RegisterMessage(&template.UnwitnessedMessage{})
	witnessedMessageMsgID = network.RegisterMessage(&template.WitnessedMessage{})
	acknowledgementMessageMsgID = network.RegisterMessage(&template.AcknowledgementMessage{})
	nodeJoinRequestMessageMsgID = network.RegisterMessage(&template.NodeJoinRequest{})
	nodeJoinResponseMessageMsgID = network.RegisterMessage(&template.NodeJoinResponse{})
	nodeJoinConfirmationMessageMsgID = network.RegisterMessage(&template.NodeJoinConfirmation{})
	nodeJoinAdmissionCommitteeMessageMsgID = network.RegisterMessage(&template.JoinAdmissionCommittee{})
}

type Service struct {
	// We need to embed the ServiceProcessor, so that incoming messages
	// are correctly handled.
	*onet.ServiceProcessor
	nodeDelays [][]int
	storage    *storage

	maxConsensusRounds int

	multiCastRounds int

	tempPingConsensus []string

	pingConsensus [][]int

	admissionCommittee []*network.ServerIdentity
	tempNewCommittee   []*network.ServerIdentity
	newNodes           []*network.ServerIdentity

	rosterNodes []*network.ServerIdentity

	step     int
	stepLock *sync.Mutex

	name                  string
	vectorClockMemberList []string

	maxNodeCount int

	majority int

	maxTime int

	active bool

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

	recievedThresholdStepWitnessedMessages     map[int][]*template.WitnessedMessage
	recievedThresholdStepWitnessedMessagesLock *sync.Mutex

	recievedAcksBool     map[int]bool
	recievedAcksBoolLock *sync.Mutex

	recievedWitnessedMessagesBool     map[int]bool
	recievedWitnessedMessagesBoolLock *sync.Mutex

	sent     [][]int
	sentLock *sync.Mutex

	deliv     []int
	delivLock *sync.Mutex

	bufferedUnwitnessedMessages     []*template.UnwitnessedMessage
	bufferedUnwitnessedMessagesLock *sync.Mutex

	bufferedWitnessedMessages     []*template.WitnessedMessage
	bufferedWitnessedMessagesLock *sync.Mutex

	bufferedAckMessages     []*template.AcknowledgementMessage
	bufferedAckMessagesLock *sync.Mutex

	bufferedCatchupMessages     []*template.CatchUpMessage
	bufferedCatchupMessagesLock *sync.Mutex

	receivedNodeJoinResponse    []*template.NodeJoinResponse
	receivedEnoughNodeResponses bool

	receivedAdmissionCommitteeJoin bool
}

var storageID = []byte("main")

type storage struct {
	Count int
	sync.Mutex
}

func findIndexOf(array []string, item string) int {
	for i := 0; i < len(array); i++ {
		if array[i] == item {
			return i
		}
	}
	return -1
}

func broadcastUnwitnessedMessage(memberNodes []*network.ServerIdentity, s *Service, message *template.UnwitnessedMessage) {
	for _, node := range memberNodes {
		e := s.SendRaw(node, message)

		senderIndex := findIndexOf(s.vectorClockMemberList, s.name)
		receiverIndex := findIndexOf(s.vectorClockMemberList, string(node.Address))

		s.sent[senderIndex][receiverIndex] = s.sent[senderIndex][receiverIndex] + 1

		if e != nil {
			panic(e)
		}
	}
}

func broadcastWitnessedMessage(memberNodes []*network.ServerIdentity, s *Service, message *template.WitnessedMessage) {
	for _, node := range memberNodes {
		e := s.SendRaw(node, message)

		senderIndex := findIndexOf(s.vectorClockMemberList, s.name)
		receiverIndex := findIndexOf(s.vectorClockMemberList, string(node.Address))

		s.sent[senderIndex][receiverIndex] = s.sent[senderIndex][receiverIndex] + 1

		if e != nil {
			panic(e)
		}
	}
}

func unicastAcknowledgementMessage(memberNode *network.ServerIdentity, s *Service, message *template.AcknowledgementMessage) {
	e := s.SendRaw(memberNode, message)

	senderIndex := findIndexOf(s.vectorClockMemberList, s.name)
	receiverIndex := findIndexOf(s.vectorClockMemberList, string(memberNode.Address))

	s.sent[senderIndex][receiverIndex] = s.sent[senderIndex][receiverIndex] + 1

	if e != nil {
		panic(e)
	}

}

func unicastCommitteJoinMessage(memberNode *network.ServerIdentity, s *Service, message *template.JoinAdmissionCommittee) {
	e := s.SendRaw(memberNode, message)
	if e != nil {
		panic(e)
	}

}

func broadcastJoinRequest(memberNodes []*network.ServerIdentity, s *Service, message *template.NodeJoinRequest) {
	for _, node := range memberNodes {
		e := s.SendRaw(node, message)
		if e != nil {
			panic(e)
		}
	}
}

func broadcastNodeJoinConfirmationMessage(memberNodes []*network.ServerIdentity, s *Service, message *template.NodeJoinConfirmation) {
	for _, node := range memberNodes {
		e := s.SendRaw(node, message)
		if e != nil {
			panic(e)
		}
	}
}

func unicastNodeResponseMessage(memberNode *network.ServerIdentity, s *Service, message *template.NodeJoinResponse) {
	e := s.SendRaw(memberNode, message)

	if e != nil {
		panic(e)
	}

}

func (s *Service) JoinRequest(req *template.JoinRequest) (*template.JoinResponse, error) {
	s.stepLock.Lock()
	nodeJoinRequest := &template.NodeJoinRequest{Id: s.ServerIdentity()}

	s.receivedNodeJoinResponse = make([]*template.NodeJoinResponse, 0)
	s.receivedEnoughNodeResponses = false

	broadcastJoinRequest(s.admissionCommittee, s, nodeJoinRequest)

	return &template.JoinResponse{}, nil

}

func (s *Service) InitRequest(req *template.InitRequest) (*template.InitResponse, error) {

	defer s.stepLock.Unlock()
	s.stepLock.Lock()

	unwitnessedMessage := &template.UnwitnessedMessage{Step: s.step,
		Id:          s.ServerIdentity(),
		SentArray:   convertInt2DtoString1D(s.sent, s.maxNodeCount, s.maxNodeCount),
		Messagetype: 0}

	broadcastUnwitnessedMessage(s.admissionCommittee, s, unwitnessedMessage)

	s.sentUnwitnessMessages[s.step] = unwitnessedMessage // check syncMaps in go

	return &template.InitResponse{}, nil

}

func (s *Service) SetGenesisSet(req *template.GenesisNodesRequest) (*template.GenesisNodesResponse, error) {

	defer s.stepLock.Unlock()
	s.stepLock.Lock()
	strNodes := req.Nodes
	s.admissionCommittee = make([]*network.ServerIdentity, 0)
	for i := 0; i < len(strNodes); i++ {
		intNode, _ := strconv.Atoi(strNodes[i])
		s.admissionCommittee = append(s.admissionCommittee, s.rosterNodes[intNode])
	}

	s.majority = len(s.admissionCommittee)

	s.sent = make([][]int, s.maxNodeCount)

	for i := 0; i < s.maxNodeCount; i++ {
		s.sent[i] = make([]int, s.maxNodeCount)
		for j := 0; j < s.maxNodeCount; j++ {
			s.sent[i][j] = 0
		}
	}

	s.deliv = make([]int, s.maxNodeCount)

	for j := 0; j < s.maxNodeCount; j++ {
		s.deliv[j] = 0
	}

	s.name = string(s.ServerIdentity().Address)

	s.vectorClockMemberList = make([]string, s.maxNodeCount)

	for i := 0; i < len(s.admissionCommittee); i++ {
		s.vectorClockMemberList[i] = string(s.admissionCommittee[i].Address)
	}
	for i := len(s.admissionCommittee); i < s.maxNodeCount; i++ {
		s.vectorClockMemberList[i] = ""
	}

	fmt.Printf("%s set the genesis set \n", s.name)

	return &template.GenesisNodesResponse{}, nil
}

func (s *Service) SetActive(req *template.ActiveStatusRequest) (*template.ActiveStatusResponse, error) {

	defer s.stepLock.Unlock()
	s.stepLock.Lock()

	s.active = false

	return &template.ActiveStatusResponse{}, nil
}

func (s *Service) SetRoster(req *template.RosterNodesRequest) (*template.RosterNodesResponse, error) {

	defer s.stepLock.Unlock()
	s.stepLock.Lock()

	s.rosterNodes = req.Roster.List

	s.nodeDelays = make([][]int, s.maxNodeCount)

	for i := 0; i < s.maxNodeCount; i++ {
		s.nodeDelays[i] = make([]int, s.maxNodeCount)
		for j := 0; j < s.maxNodeCount; j++ {
			s.nodeDelays[i][j] = 20
		}
	}

	// delay node 1

	for i := 0; i < s.maxNodeCount; i++ {
		s.nodeDelays[i][1] = 100
		s.nodeDelays[i][2] = 100
		s.nodeDelays[i][3] = 100
		s.nodeDelays[i][4] = 100
		s.nodeDelays[i][5] = 100
		s.nodeDelays[i][6] = 100
		s.nodeDelays[i][7] = 100
	}

	return &template.RosterNodesResponse{}, nil
}

func (s *Service) NewProtocol(tn *onet.TreeNodeInstance, conf *onet.GenericConfig) (onet.ProtocolInstance, error) {
	log.Lvl3("Not templated yet")
	return nil, nil
}

func (s *Service) save() {
	s.storage.Lock()
	defer s.storage.Unlock()
	err := s.Save(storageID, s.storage)
	if err != nil {
		log.Error("Couldn't save data:", err)
	}
}

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

func convertInt2DtoString1D(array [][]int, rows int, cols int) []string {
	stringArray := make([]string, rows*cols)
	for i := 0; i < rows; i++ {
		for j := 0; j < cols; j++ {
			stringArray[i*rows+j] = strconv.Itoa(array[i][j])
		}
	}
	return stringArray

}

func convertString1DtoInt2D(array []string, rows int, cols int) [][]int {
	intArray := make([][]int, rows)
	for i := 0; i < rows; i++ {
		intArray[i] = make([]int, cols)
	}

	for i := 0; i < rows; i++ {
		for j := 0; j < cols; j++ {
			intArray[i][j], _ = strconv.Atoi(array[i*rows+j])
		}
	}

	return intArray

}

func findGlobalMaxRandomNumber(messages []*template.WitnessedMessage) int {
	globalMax := 0
	for i := 0; i < len(messages); i++ {
		if messages[i].RandomNumber > globalMax {
			globalMax = messages[i].RandomNumber
		}
	}
	return globalMax
}

func unique(stringSlice []string) []string {
	keys := make(map[string]bool)
	list := []string{}
	for _, entry := range stringSlice {
		if _, value := keys[entry]; !value {
			keys[entry] = true
			list = append(list, entry)
		}
	}
	return list
}

func max(num1 int, num2 int) int {
	if num1 >= num2 {
		return num1
	} else {
		return num2
	}
}

func convertIntArraytoStringArray(values []int) []string {
	stringArray := make([]string, len(values))
	for i := 0; i < len(values); i++ {
		stringArray[i] = strconv.Itoa(values[i])
	}
	return stringArray
}

func convertStringArraytoIntArray(values []string) []int {
	intValues := make([]int, len(values))
	for i := 0; i < len(values); i++ {
		intValues[i], _ = strconv.Atoi(values[i])
	}
	return intValues
}

func handleUnwitnessedMessage(s *Service, req *template.UnwitnessedMessage) {

	reqSentArray := convertString1DtoInt2D(req.SentArray, s.maxNodeCount, s.maxNodeCount)

	for i := 0; i < s.maxNodeCount; i++ {
		for j := 0; j < s.maxNodeCount; j++ {
			s.sent[i][j] = max(s.sent[i][j], reqSentArray[i][j])
		}
	}

	reqIndex := findIndexOf(s.vectorClockMemberList, string(req.Id.Address))
	for reqIndex == -1 {
		reqIndex = findIndexOf(s.vectorClockMemberList, string(req.Id.Address))
	}

	s.deliv[reqIndex] = s.deliv[reqIndex] + 1

	stepNow := s.step

	if req.Step <= stepNow {

		s.recievedUnwitnessedMessages[req.Step] = append(s.recievedUnwitnessedMessages[req.Step], req)
		newAck := &template.AcknowledgementMessage{Id: s.ServerIdentity(),
			UnwitnessedMessage: req,
			SentArray:          convertInt2DtoString1D(s.sent, s.maxNodeCount, s.maxNodeCount)}
		requesterIdentity := req.Id
		unicastAcknowledgementMessage(requesterIdentity, s, newAck)
		s.sentAcknowledgementMessages[req.Step] = append(s.sentAcknowledgementMessages[req.Step], newAck)

	} else if req.Step > stepNow {
		s.recievedTempUnwitnessedMessages[req.Step] = append(s.recievedTempUnwitnessedMessages[req.Step], req)
	}

}

func handleAckMessage(s *Service, req *template.AcknowledgementMessage) {

	reqSentArray := convertString1DtoInt2D(req.SentArray, s.maxNodeCount, s.maxNodeCount)

	for i := 0; i < s.maxNodeCount; i++ {
		for j := 0; j < s.maxNodeCount; j++ {
			s.sent[i][j] = max(s.sent[i][j], reqSentArray[i][j])
		}
	}

	reqIndex := findIndexOf(s.vectorClockMemberList, string(req.Id.Address))

	s.deliv[reqIndex] = s.deliv[reqIndex] + 1

	s.recievedAcknowledgesMessages[req.UnwitnessedMessage.Step] = append(s.recievedAcknowledgesMessages[req.UnwitnessedMessage.Step], req)
	stepNow := s.step

	hasEnoughAcks := s.recievedAcksBool[stepNow]

	if !hasEnoughAcks {

		lenRecievedAcks := len(s.recievedAcknowledgesMessages[stepNow])

		if lenRecievedAcks >= s.majority {

			nodes := make([]*network.ServerIdentity, 0)
			for _, ack := range s.recievedAcknowledgesMessages[stepNow] {
				ackId := ack.Id
				exists := false
				for _, num := range nodes {

					if string(num.Address) == string(ackId.Address) {
						exists = true
						break
					}
				}
				if !exists {
					nodes = append(nodes, ackId)
				}
			}
			if len(nodes) >= s.majority {
				s.recievedAcksBool[stepNow] = true

				var newWitness *template.WitnessedMessage

				if s.sentUnwitnessMessages[stepNow].Messagetype == 0 {
					newWitness = &template.WitnessedMessage{Step: stepNow,
						Id:          s.ServerIdentity(),
						SentArray:   convertInt2DtoString1D(s.sent, s.maxNodeCount, s.maxNodeCount),
						Messagetype: 0}
				}
				if s.sentUnwitnessMessages[stepNow].Messagetype == 1 {
					newWitness = &template.WitnessedMessage{Step: stepNow,
						Id:                       s.ServerIdentity(),
						SentArray:                convertInt2DtoString1D(s.sent, s.maxNodeCount, s.maxNodeCount),
						Messagetype:              1,
						PingDistances:            s.sentUnwitnessMessages[stepNow].PingDistances,
						PingMulticastRoundNumber: s.sentUnwitnessMessages[stepNow].PingMulticastRoundNumber}
				}
				if s.sentUnwitnessMessages[stepNow].Messagetype == 2 {
					if s.sentUnwitnessMessages[stepNow].ConsensusStepNumber == 0 {
						newWitness = &template.WitnessedMessage{Step: stepNow,
							Id:                   s.ServerIdentity(),
							SentArray:            convertInt2DtoString1D(s.sent, s.maxNodeCount, s.maxNodeCount),
							RandomNumber:         s.sentUnwitnessMessages[s.step].RandomNumber,
							NodesProposal:        s.sentUnwitnessMessages[s.step].NodesProposal,
							ConsensusRoundNumber: s.sentUnwitnessMessages[s.step].ConsensusRoundNumber,
							Messagetype:          2,
							ConsensusStepNumber:  s.sentUnwitnessMessages[s.step].ConsensusStepNumber,
							FoundConsensus:       s.sentUnwitnessMessages[s.step].FoundConsensus}
					}
					if s.sentUnwitnessMessages[stepNow].ConsensusStepNumber == 1 {
						newWitness = &template.WitnessedMessage{Step: stepNow,
							Id:                   s.ServerIdentity(),
							SentArray:            convertInt2DtoString1D(s.sent, s.maxNodeCount, s.maxNodeCount),
							Nodes:                s.sentUnwitnessMessages[stepNow].Nodes,
							ConsensusRoundNumber: s.sentUnwitnessMessages[s.step].ConsensusRoundNumber,
							Messagetype:          2,
							ConsensusStepNumber:  s.sentUnwitnessMessages[s.step].ConsensusStepNumber,
							FoundConsensus:       s.sentUnwitnessMessages[s.step].FoundConsensus}
					}
					if s.sentUnwitnessMessages[stepNow].ConsensusStepNumber == 2 {
						newWitness = &template.WitnessedMessage{Step: stepNow,
							Id:                   s.ServerIdentity(),
							SentArray:            convertInt2DtoString1D(s.sent, s.maxNodeCount, s.maxNodeCount),
							ConsensusRoundNumber: s.sentUnwitnessMessages[s.step].ConsensusRoundNumber,
							Messagetype:          2,
							ConsensusStepNumber:  s.sentUnwitnessMessages[s.step].ConsensusStepNumber,
							FoundConsensus:       s.sentUnwitnessMessages[s.step].FoundConsensus}
					}
				}
				if s.sentUnwitnessMessages[stepNow].Messagetype == 3 {
					if s.sentUnwitnessMessages[stepNow].ConsensusStepNumber == 0 {
						newWitness = &template.WitnessedMessage{Step: stepNow,
							Id:                   s.ServerIdentity(),
							SentArray:            convertInt2DtoString1D(s.sent, s.maxNodeCount, s.maxNodeCount),
							RandomNumber:         s.sentUnwitnessMessages[s.step].RandomNumber,
							PingMetrix:           s.sentUnwitnessMessages[s.step].PingMetrix,
							ConsensusRoundNumber: s.sentUnwitnessMessages[s.step].ConsensusRoundNumber,
							Messagetype:          3,
							ConsensusStepNumber:  s.sentUnwitnessMessages[s.step].ConsensusStepNumber,
							FoundConsensus:       s.sentUnwitnessMessages[s.step].FoundConsensus}
					}
					if s.sentUnwitnessMessages[stepNow].ConsensusStepNumber == 1 {
						newWitness = &template.WitnessedMessage{Step: stepNow,
							Id:                   s.ServerIdentity(),
							SentArray:            convertInt2DtoString1D(s.sent, s.maxNodeCount, s.maxNodeCount),
							Nodes:                s.sentUnwitnessMessages[stepNow].Nodes,
							ConsensusRoundNumber: s.sentUnwitnessMessages[s.step].ConsensusRoundNumber,
							Messagetype:          3,
							ConsensusStepNumber:  s.sentUnwitnessMessages[s.step].ConsensusStepNumber,
							FoundConsensus:       s.sentUnwitnessMessages[s.step].FoundConsensus}
					}
					if s.sentUnwitnessMessages[stepNow].ConsensusStepNumber == 2 {
						newWitness = &template.WitnessedMessage{Step: stepNow,
							Id:                   s.ServerIdentity(),
							SentArray:            convertInt2DtoString1D(s.sent, s.maxNodeCount, s.maxNodeCount),
							ConsensusRoundNumber: s.sentUnwitnessMessages[s.step].ConsensusRoundNumber,
							Messagetype:          3,
							ConsensusStepNumber:  s.sentUnwitnessMessages[s.step].ConsensusStepNumber,
							FoundConsensus:       s.sentUnwitnessMessages[s.step].FoundConsensus}
					}
				}
				broadcastWitnessedMessage(s.admissionCommittee, s, newWitness)
				s.sentThresholdWitnessedMessages[stepNow] = newWitness
			}
		}
	}
}

func handleWitnessedMessage(s *Service, req *template.WitnessedMessage) {

	reqSentArray := convertString1DtoInt2D(req.SentArray, s.maxNodeCount, s.maxNodeCount)

	for i := 0; i < s.maxNodeCount; i++ {
		for j := 0; j < s.maxNodeCount; j++ {
			s.sent[i][j] = max(s.sent[i][j], reqSentArray[i][j])
		}
	}

	reqIndex := findIndexOf(s.vectorClockMemberList, string(req.Id.Address))

	s.deliv[reqIndex] = s.deliv[reqIndex] + 1

	stepNow := s.step
	s.recievedThresholdwitnessedMessages[req.Step] = append(s.recievedThresholdwitnessedMessages[req.Step], req)

	hasRecievedWitnessedMessages := s.recievedWitnessedMessagesBool[stepNow]

	if !hasRecievedWitnessedMessages {

		lenThresholdWitnessedMessages := len(s.recievedThresholdwitnessedMessages[stepNow])

		if lenThresholdWitnessedMessages >= s.majority {

			nodes := make([]*network.ServerIdentity, 0)
			for _, twm := range s.recievedThresholdwitnessedMessages[stepNow] {
				twmId := twm.Id

				exists := false
				for _, num := range nodes {
					if string(num.Address) == string(twmId.Address) {
						exists = true
						break
					}
				}
				if !exists {
					nodes = append(nodes, twmId)
				}
			}

			if len(nodes) >= s.majority {
				s.recievedWitnessedMessagesBool[stepNow] = true
				for _, nodeId := range nodes {
					for _, twm := range s.recievedThresholdwitnessedMessages[stepNow] {
						if string(nodeId.Address) == string(twm.Id.Address) {
							s.recievedThresholdStepWitnessedMessages[stepNow] = append(s.recievedThresholdStepWitnessedMessages[stepNow], twm)
							break
						}
					}
				}

				s.step = s.step + 1
				stepNow = s.step
				fmt.Printf("%s increased time step to %d \n", s.ServerIdentity(), stepNow)

				var unwitnessedMessage *template.UnwitnessedMessage
				if stepNow == 1 {
					s.majority = len(s.admissionCommittee)/2 + 1
					// start membership consensus
					randomNumber := rand.Intn(s.maxNodeCount * 10000)
					fmt.Printf("%s started the membership consensus process with initial random number is %d \n", s.ServerIdentity(), randomNumber)
					s.tempNewCommittee = append(s.admissionCommittee, s.newNodes...)
					s.newNodes = make([]*network.ServerIdentity, 0)
					nodes := s.tempNewCommittee
					strNodes := make([]string, len(nodes))
					for r := 0; r < len(nodes); r++ {
						for p := 0; p < len(s.rosterNodes); p++ {
							if string(s.rosterNodes[p].Address) == string(nodes[r].Address) {
								strNodes[r] = strconv.Itoa(p)
								break
							}
						}
					}

					unwitnessedMessage = &template.UnwitnessedMessage{Step: s.step,
						Id:                   s.ServerIdentity(),
						SentArray:            convertInt2DtoString1D(s.sent, s.maxNodeCount, s.maxNodeCount),
						NodesProposal:        strNodes,
						RandomNumber:         randomNumber,
						ConsensusRoundNumber: s.maxConsensusRounds,
						Messagetype:          2,
						ConsensusStepNumber:  0,
						FoundConsensus:       false}
				}
				if stepNow > 1 {
					if s.sentUnwitnessMessages[stepNow-1].Messagetype == 0 {
						unwitnessedMessage = &template.UnwitnessedMessage{Step: stepNow,
							Id:          s.ServerIdentity(),
							SentArray:   convertInt2DtoString1D(s.sent, s.maxNodeCount, s.maxNodeCount),
							Messagetype: 0}
					}
					if s.sentUnwitnessMessages[stepNow-1].Messagetype == 1 {
						// ping distance multicast
						if s.sentUnwitnessMessages[stepNow-1].PingMulticastRoundNumber > 1 {
							unwitnessedMessage = &template.UnwitnessedMessage{Step: stepNow,
								Id:                       s.ServerIdentity(),
								SentArray:                convertInt2DtoString1D(s.sent, s.maxNodeCount, s.maxNodeCount),
								Messagetype:              1,
								PingDistances:            s.sentUnwitnessMessages[stepNow-1].PingDistances,
								PingMulticastRoundNumber: s.sentUnwitnessMessages[stepNow-1].PingMulticastRoundNumber - 1}
						} else {
							// make the ping distance matrix and start the ping consensus protocol
							pingMatrix := make([][]int, len(s.admissionCommittee))
							for i := 0; i < len(s.admissionCommittee); i++ {
								found := false
								q := 1
								for found == false && q < s.multiCastRounds {
									for l := 0; l < len(s.recievedThresholdwitnessedMessages[stepNow-q]); l++ {
										if string(s.recievedThresholdwitnessedMessages[stepNow-q][l].Id.Address) == string(s.admissionCommittee[i].Address) &&
											s.recievedThresholdwitnessedMessages[stepNow-q][l].PingDistances != nil &&
											len(s.recievedThresholdwitnessedMessages[stepNow-q][l].PingDistances) > 0 {
											pingMatrix[i] = convertStringArraytoIntArray(s.recievedThresholdwitnessedMessages[stepNow-q][l].PingDistances)
											found = true
											break
										}
									}
									q = q + 1
								}
								if found == false {
									pingMatrix[i] = make([]int, len(s.admissionCommittee))
									for b := 0; b < len(s.admissionCommittee); b++ {
										pingMatrix[i][b] = -1
									}
								}
							}
							pingMatrixStr := convertInt2DtoString1D(pingMatrix, len(s.admissionCommittee), len(s.admissionCommittee))
							s.tempPingConsensus = pingMatrixStr
							randomNumber := rand.Intn(10000)

							fmt.Printf("%s's initial ping proposal random number is %d \n", s.ServerIdentity(), randomNumber)

							unwitnessedMessage = &template.UnwitnessedMessage{Step: s.step,
								Id:                   s.ServerIdentity(),
								SentArray:            convertInt2DtoString1D(s.sent, s.maxNodeCount, s.maxNodeCount),
								PingMetrix:           pingMatrixStr,
								RandomNumber:         randomNumber,
								ConsensusRoundNumber: s.maxConsensusRounds,
								Messagetype:          3,
								ConsensusStepNumber:  0,
								FoundConsensus:       false}

						}
					}
					if s.sentUnwitnessMessages[stepNow-1].Messagetype == 2 {
						if s.sentUnwitnessMessages[stepNow-1].ConsensusStepNumber == 0 {
							strNodes := make([]string, len(nodes))
							for r := 0; r < len(nodes); r++ {
								for p := 0; p < len(s.rosterNodes); p++ {
									if string(s.rosterNodes[p].Address) == string(nodes[r].Address) {
										strNodes[r] = strconv.Itoa(p)
										break
									}
								}
							}
							unwitnessedMessage = &template.UnwitnessedMessage{Step: stepNow,
								Id:                   s.ServerIdentity(),
								SentArray:            convertInt2DtoString1D(s.sent, s.maxNodeCount, s.maxNodeCount),
								Nodes:                strNodes,
								ConsensusRoundNumber: s.sentUnwitnessMessages[stepNow-1].ConsensusRoundNumber,
								Messagetype:          2,
								ConsensusStepNumber:  1,
								FoundConsensus:       s.sentUnwitnessMessages[stepNow-1].FoundConsensus}
						}
						if s.sentUnwitnessMessages[stepNow-1].ConsensusStepNumber == 1 {
							if s.sentUnwitnessMessages[stepNow-1].ConsensusRoundNumber < s.maxConsensusRounds {
								// there has been at least one finished consensus round
								numSeenConsensus := 0
								for _, twm := range s.recievedThresholdwitnessedMessages[stepNow-2] {
									if twm.FoundConsensus == true {
										numSeenConsensus++
									}
								}
								if numSeenConsensus > 0 {
									// someone has seen the consensus, so let's move on
									fmt.Printf("The nodes reach the node consenses in %d number of rounds\n", s.maxConsensusRounds-s.sentUnwitnessMessages[stepNow-1].ConsensusRoundNumber)
									for _, twm := range s.recievedThresholdwitnessedMessages[stepNow-2] {
										if twm.FoundConsensus == true {
											s.admissionCommittee = make([]*network.ServerIdentity, 0)
											for u := 0; u < len(twm.NodesProposal); u++ {
												index, _ := strconv.Atoi(twm.NodesProposal[u])
												s.admissionCommittee = append(s.admissionCommittee, s.rosterNodes[index])
											}
											fmt.Printf("Node %s reached early nodes consenses with %s and updated the roster \n", s.name, s.admissionCommittee)
											break
										}
									}
									s.majority = len(s.admissionCommittee)/2 + 1

									for i := 0; i < len(s.admissionCommittee); i++ {
										isNewNode := true
										for j := 0; j < len(s.vectorClockMemberList); j++ {
											if s.vectorClockMemberList[j] == string(s.admissionCommittee[i].Address) {
												isNewNode = false
												break
											}
										}

										if isNewNode {
											for j := 0; j < len(s.vectorClockMemberList); j++ {
												if s.vectorClockMemberList[j] == "" {
													s.vectorClockMemberList[j] = string(s.admissionCommittee[i].Address)
													break
												}
											}
											// send a message to the newNode

											NewAdmissionCommitteenodes := s.admissionCommittee
											strNodes := make([]string, len(NewAdmissionCommitteenodes))
											for r := 0; r < len(NewAdmissionCommitteenodes); r++ {
												for p := 0; p < len(s.rosterNodes); p++ {
													if string(s.rosterNodes[p].Address) == string(NewAdmissionCommitteenodes[r].Address) {
														strNodes[r] = strconv.Itoa(p)
														break
													}
												}
											}

											joinCommitteMessage := &template.JoinAdmissionCommittee{Step: stepNow, NewCommitee: strNodes}
											unicastCommitteJoinMessage(s.admissionCommittee[i], s, joinCommitteMessage)
										}

									}

									time.Sleep(2 * time.Second)

									// calculate ping distances, for now lets mock the ping distances

									pingDistances := make([]int, len(s.admissionCommittee))
									for i := 0; i < len(s.admissionCommittee); i++ {
										pingDistances[i] = rand.Intn(300)
									}

									unwitnessedMessage = &template.UnwitnessedMessage{Step: stepNow,
										Id:                       s.ServerIdentity(),
										SentArray:                convertInt2DtoString1D(s.sent, s.maxNodeCount, s.maxNodeCount),
										Messagetype:              1,
										PingDistances:            convertIntArraytoStringArray(pingDistances),
										PingMulticastRoundNumber: s.multiCastRounds}

								} else {
									unwitnessedMessage = &template.UnwitnessedMessage{Step: stepNow,
										Id:                   s.ServerIdentity(),
										SentArray:            convertInt2DtoString1D(s.sent, s.maxNodeCount, s.maxNodeCount),
										ConsensusRoundNumber: s.sentUnwitnessMessages[stepNow-1].ConsensusRoundNumber,
										Messagetype:          2,
										ConsensusStepNumber:  2,
										FoundConsensus:       s.sentUnwitnessMessages[stepNow-1].FoundConsensus}
								}

							} else {
								unwitnessedMessage = &template.UnwitnessedMessage{Step: stepNow,
									Id:                   s.ServerIdentity(),
									SentArray:            convertInt2DtoString1D(s.sent, s.maxNodeCount, s.maxNodeCount),
									ConsensusRoundNumber: s.sentUnwitnessMessages[stepNow-1].ConsensusRoundNumber,
									Messagetype:          2,
									ConsensusStepNumber:  2,
									FoundConsensus:       s.sentUnwitnessMessages[stepNow-1].FoundConsensus}
							}

						}
						if s.sentUnwitnessMessages[stepNow-1].ConsensusStepNumber == 2 {
							consensusFound := false
							consensusValue := make([]string, 0)
							celebrityMessages := s.recievedThresholdStepWitnessedMessages[stepNow-2]
							randomNumber := -1
							properConsensus := false

							CelibrityNodes := make([]string, 0)
							for i := 0; i < len(celebrityMessages); i++ {
								CelibrityNodes = append(CelibrityNodes, celebrityMessages[i].Nodes...) //possible duplicates
							}

							CelibrityNodes = unique(CelibrityNodes)

							globalMaxRandomNumber := findGlobalMaxRandomNumber(s.recievedThresholdwitnessedMessages[stepNow-3])

							for i := 0; i < len(CelibrityNodes); i++ {
								nodeIndex, _ := strconv.Atoi(CelibrityNodes[i])
								for j := 0; j < len(s.recievedThresholdwitnessedMessages[stepNow-3]); j++ {

									if string(s.rosterNodes[nodeIndex].Address) == string(s.recievedThresholdwitnessedMessages[stepNow-3][j].Id.Address) {

										if s.recievedThresholdwitnessedMessages[stepNow-3][j].RandomNumber == globalMaxRandomNumber {
											consensusFound = true
											consensusValue = s.recievedThresholdwitnessedMessages[stepNow-3][j].NodesProposal
											randomNumber = s.recievedThresholdwitnessedMessages[stepNow-3][j].RandomNumber
											properConsensus = true
										}

										break
									}
								}
								if consensusFound {
									break
								}
							}

							if !consensusFound {
								witnessedMessages1 := s.recievedThresholdwitnessedMessages[stepNow-2]
								CelibrityNodes1 := make([]string, 0)
								for i := 0; i < len(witnessedMessages1); i++ {
									CelibrityNodes1 = append(CelibrityNodes1, witnessedMessages1[i].Nodes...) //possible duplicates
								}

								CelibrityNodes1 = unique(CelibrityNodes1)

								for i := 0; i < len(CelibrityNodes1); i++ {
									nodeIndex, _ := strconv.Atoi(CelibrityNodes1[i])
									for j := 0; j < len(s.recievedThresholdwitnessedMessages[stepNow-3]); j++ {

										if string(s.rosterNodes[nodeIndex].Address) == string(s.recievedThresholdwitnessedMessages[stepNow-3][j].Id.Address) {

											if s.recievedThresholdwitnessedMessages[stepNow-3][j].RandomNumber == globalMaxRandomNumber {
												consensusFound = true
												consensusValue = s.recievedThresholdwitnessedMessages[stepNow-3][j].NodesProposal
												randomNumber = s.recievedThresholdwitnessedMessages[stepNow-3][j].RandomNumber
												properConsensus = false
											}

											break
										}
									}

									if consensusFound {
										break
									}

								}

							}

							if consensusFound {
								fmt.Printf("Found consensus with random number %d and the consensus property is %s with length %d \n", randomNumber, properConsensus, len(consensusValue))
								s.tempNewCommittee = make([]*network.ServerIdentity, 0)
								for u := 0; u < len(consensusValue); u++ {
									index, _ := strconv.Atoi(consensusValue[u])
									s.tempNewCommittee = append(s.tempNewCommittee, s.rosterNodes[index])
								}
							} else {
								fmt.Printf("Did not find consensus\n")
							}

							if s.sentUnwitnessMessages[stepNow-1].ConsensusRoundNumber > 1 {

								strNodes := make([]string, 0)
								if consensusFound {
									strNodes = consensusValue
								} else if s.tempNewCommittee != nil && len(s.tempNewCommittee) > 0 {
									strNodes = make([]string, len(s.tempNewCommittee))
									for r := 0; r < len(s.tempNewCommittee); r++ {
										for p := 0; p < len(s.rosterNodes); p++ {
											if string(s.rosterNodes[p].Address) == string(s.tempNewCommittee[r].Address) {
												strNodes[r] = strconv.Itoa(p)
												break
											}
										}
									}

								}
								randomNumber := rand.Intn(s.maxNodeCount * 10000)

								fmt.Printf("%s's new proposal random number is %d \n", s.ServerIdentity(), randomNumber)

								unwitnessedMessage = &template.UnwitnessedMessage{Step: stepNow,
									Id:                   s.ServerIdentity(),
									SentArray:            convertInt2DtoString1D(s.sent, s.maxNodeCount, s.maxNodeCount),
									NodesProposal:        strNodes,
									RandomNumber:         randomNumber,
									ConsensusRoundNumber: s.sentUnwitnessMessages[stepNow-1].ConsensusRoundNumber - 1,
									Messagetype:          2,
									ConsensusStepNumber:  0,
									FoundConsensus:       consensusFound}

							} else {
								// end of consensus rounds
								if s.tempNewCommittee != nil && s.sentUnwitnessMessages[stepNow-1].ConsensusRoundNumber == 0 {

									s.admissionCommittee = s.tempNewCommittee

									s.majority = len(s.admissionCommittee)/2 + 1

									for i := 0; i < len(s.admissionCommittee); i++ {
										isNewNode := true
										for j := 0; j < len(s.vectorClockMemberList); j++ {
											if s.vectorClockMemberList[j] == string(s.admissionCommittee[i].Address) {
												isNewNode = false
												break
											}
										}

										if isNewNode {
											for j := 0; j < len(s.vectorClockMemberList); j++ {
												if s.vectorClockMemberList[j] == "" {
													s.vectorClockMemberList[j] = string(s.admissionCommittee[i].Address)
													break
												}
											}
											NewAdmissionCommitteenodes := s.admissionCommittee
											strNodes := make([]string, len(NewAdmissionCommitteenodes))
											for r := 0; r < len(NewAdmissionCommitteenodes); r++ {
												for p := 0; p < len(s.rosterNodes); p++ {
													if string(s.rosterNodes[p].Address) == string(NewAdmissionCommitteenodes[r].Address) {
														strNodes[r] = strconv.Itoa(p)
														break
													}
												}
											}

											joinCommitteMessage := &template.JoinAdmissionCommittee{Step: stepNow, NewCommitee: strNodes}
											unicastCommitteJoinMessage(s.admissionCommittee[i], s, joinCommitteMessage)
										}

									}

									time.Sleep(2 * time.Second)

									// calculate ping distances, for now lets mock the ping distances

									pingDistances := make([]int, len(s.admissionCommittee))
									for i := 0; i < len(s.admissionCommittee); i++ {
										pingDistances[i] = rand.Intn(300)
									}

									unwitnessedMessage = &template.UnwitnessedMessage{Step: stepNow,
										Id:                       s.ServerIdentity(),
										SentArray:                convertInt2DtoString1D(s.sent, s.maxNodeCount, s.maxNodeCount),
										Messagetype:              1,
										PingDistances:            convertIntArraytoStringArray(pingDistances),
										PingMulticastRoundNumber: s.multiCastRounds}

									s.tempNewCommittee = nil

								}

							}

						}

					}
					if s.sentUnwitnessMessages[stepNow-1].Messagetype == 3 {
						if s.sentUnwitnessMessages[stepNow-1].ConsensusStepNumber == 0 {
							strNodes := make([]string, len(nodes))
							for r := 0; r < len(nodes); r++ {
								for p := 0; p < len(s.rosterNodes); p++ {
									if string(s.rosterNodes[p].Address) == string(nodes[r].Address) {
										strNodes[r] = strconv.Itoa(p)
										break
									}
								}
							}
							unwitnessedMessage = &template.UnwitnessedMessage{Step: stepNow,
								Id:                   s.ServerIdentity(),
								SentArray:            convertInt2DtoString1D(s.sent, s.maxNodeCount, s.maxNodeCount),
								Nodes:                strNodes,
								ConsensusRoundNumber: s.sentUnwitnessMessages[stepNow-1].ConsensusRoundNumber,
								Messagetype:          3,
								ConsensusStepNumber:  1,
								FoundConsensus:       s.sentUnwitnessMessages[stepNow-1].FoundConsensus}
						}
						if s.sentUnwitnessMessages[stepNow-1].ConsensusStepNumber == 1 {
							if s.sentUnwitnessMessages[stepNow-1].ConsensusRoundNumber < s.maxConsensusRounds {
								// there have been at least one finished consensus round
								numSeenConsensus := 0

								for _, twm := range s.recievedThresholdwitnessedMessages[stepNow-2] {
									if twm.FoundConsensus == true {
										numSeenConsensus++
									}
								}

								if numSeenConsensus > 1 {
									// someone has seen the consensus, so let's move on
									for _, twm := range s.recievedThresholdwitnessedMessages[stepNow-2] {
										if twm.FoundConsensus == true {
											s.pingConsensus = convertString1DtoInt2D(twm.PingMetrix, len(s.admissionCommittee), len(s.admissionCommittee))

											fmt.Printf("%s early end of ping matrix consensus with ping matrix %s in %d consensus rounds \n", s.name, s.pingConsensus, s.maxConsensusRounds-s.sentUnwitnessMessages[stepNow-1].ConsensusRoundNumber)

											unwitnessedMessage = &template.UnwitnessedMessage{Step: stepNow,
												Id:          s.ServerIdentity(),
												SentArray:   convertInt2DtoString1D(s.sent, s.maxNodeCount, s.maxNodeCount),
												Messagetype: 0}
											break
										}
									}

								} else {
									unwitnessedMessage = &template.UnwitnessedMessage{Step: stepNow,
										Id:                   s.ServerIdentity(),
										SentArray:            convertInt2DtoString1D(s.sent, s.maxNodeCount, s.maxNodeCount),
										ConsensusRoundNumber: s.sentUnwitnessMessages[stepNow-1].ConsensusRoundNumber,
										Messagetype:          3,
										ConsensusStepNumber:  2}
								}

							} else {
								unwitnessedMessage = &template.UnwitnessedMessage{Step: stepNow,
									Id:                   s.ServerIdentity(),
									SentArray:            convertInt2DtoString1D(s.sent, s.maxNodeCount, s.maxNodeCount),
									ConsensusRoundNumber: s.sentUnwitnessMessages[stepNow-1].ConsensusRoundNumber,
									Messagetype:          3,
									ConsensusStepNumber:  2,
									FoundConsensus:       s.sentUnwitnessMessages[stepNow-1].FoundConsensus}
							}

						}
						if s.sentUnwitnessMessages[stepNow-1].ConsensusStepNumber == 2 {
							consensusFound := false
							consensusValue := make([]string, 0)
							celebrityMessages := s.recievedThresholdStepWitnessedMessages[stepNow-2]
							randomNumber := -1
							properConsensus := false

							CelibrityNodes := make([]string, 0)
							for i := 0; i < len(celebrityMessages); i++ {
								CelibrityNodes = append(CelibrityNodes, celebrityMessages[i].Nodes...) //possible duplicates
							}

							CelibrityNodes = unique(CelibrityNodes)
							globalMaxRandomNumber := findGlobalMaxRandomNumber(s.recievedThresholdwitnessedMessages[stepNow-3])

							for i := 0; i < len(CelibrityNodes); i++ {
								nodeIndex, _ := strconv.Atoi(CelibrityNodes[i])
								for j := 0; j < len(s.recievedThresholdwitnessedMessages[stepNow-3]); j++ {

									if string(s.rosterNodes[nodeIndex].Address) == string(s.recievedThresholdwitnessedMessages[stepNow-3][j].Id.Address) {

										if s.recievedThresholdwitnessedMessages[stepNow-3][j].RandomNumber == globalMaxRandomNumber {
											consensusFound = true
											consensusValue = s.recievedThresholdwitnessedMessages[stepNow-3][j].PingMetrix
											randomNumber = s.recievedThresholdwitnessedMessages[stepNow-3][j].RandomNumber
											properConsensus = true
										}

										break
									}
								}
								if consensusFound {
									break
								}
							}

							if !consensusFound {
								witnessedMessages1 := s.recievedThresholdwitnessedMessages[stepNow-2]
								CelibrityNodes1 := make([]string, 0)
								for i := 0; i < len(witnessedMessages1); i++ {
									CelibrityNodes1 = append(CelibrityNodes1, witnessedMessages1[i].Nodes...) //possible duplicates
								}

								CelibrityNodes1 = unique(CelibrityNodes1)

								for i := 0; i < len(CelibrityNodes1); i++ {
									nodeIndex, _ := strconv.Atoi(CelibrityNodes1[i])
									for j := 0; j < len(s.recievedThresholdwitnessedMessages[stepNow-3]); j++ {

										if string(s.rosterNodes[nodeIndex].Address) == string(s.recievedThresholdwitnessedMessages[stepNow-3][j].Id.Address) {

											if s.recievedThresholdwitnessedMessages[stepNow-3][j].RandomNumber == globalMaxRandomNumber {
												consensusFound = true
												consensusValue = s.recievedThresholdwitnessedMessages[stepNow-3][j].PingMetrix
												randomNumber = s.recievedThresholdwitnessedMessages[stepNow-3][j].RandomNumber
												properConsensus = false
											}

											break
										}
									}

									if consensusFound {
										break
									}

								}

							}

							if consensusFound {
								fmt.Printf("Found consensus with random number %d and the consensus property is %s \n", randomNumber, properConsensus)
								s.tempPingConsensus = consensusValue
							} else {
								fmt.Printf("Did not find consensus\n")
							}

							if s.sentUnwitnessMessages[stepNow-1].ConsensusRoundNumber > 1 {

								strPingMtrx := make([]string, 0)

								if consensusFound {
									strPingMtrx = consensusValue
								} else if s.tempPingConsensus != nil && len(s.tempPingConsensus) > 0 {
									strPingMtrx = s.tempPingConsensus
								}
								randomNumber := rand.Intn(10000)

								fmt.Printf("%s's new proposal random number is %d \n", s.ServerIdentity(), randomNumber)

								unwitnessedMessage = &template.UnwitnessedMessage{Step: stepNow,
									Id:                   s.ServerIdentity(),
									SentArray:            convertInt2DtoString1D(s.sent, s.maxNodeCount, s.maxNodeCount),
									PingMetrix:           strPingMtrx,
									RandomNumber:         randomNumber,
									ConsensusRoundNumber: s.sentUnwitnessMessages[stepNow-1].ConsensusRoundNumber - 1,
									Messagetype:          3,
									ConsensusStepNumber:  0,
									FoundConsensus:       consensusFound}

							} else {
								// end of consensus rounds

								if s.tempPingConsensus != nil && s.sentUnwitnessMessages[stepNow-1].ConsensusRoundNumber == 0 {

									s.pingConsensus = convertString1DtoInt2D(s.tempPingConsensus, len(s.admissionCommittee), len(s.admissionCommittee))

									fmt.Printf("%s end of ping matrix consensus with ping matrix %s", s.name, s.pingConsensus)

									unwitnessedMessage = &template.UnwitnessedMessage{Step: stepNow,
										Id:          s.ServerIdentity(),
										SentArray:   convertInt2DtoString1D(s.sent, s.maxNodeCount, s.maxNodeCount),
										Messagetype: 0}

								}

							}

						}
					}
				}

				if unwitnessedMessage.Messagetype == 0 {
					// end of control plane, sleep for some time, and then start a new control plane
					time.Sleep(10 * time.Second) // analogous to one CRUX round
					randomNumber := rand.Intn(s.maxNodeCount * 10000)
					fmt.Printf("%s started the membership consensus process with initial random number is %d \n", s.ServerIdentity(), randomNumber)
					s.tempNewCommittee = append(s.admissionCommittee, s.newNodes...)
					s.newNodes = make([]*network.ServerIdentity, 0)
					nodes := s.tempNewCommittee
					strNodes := make([]string, len(nodes))
					for r := 0; r < len(nodes); r++ {
						for p := 0; p < len(s.rosterNodes); p++ {
							if string(s.rosterNodes[p].Address) == string(nodes[r].Address) {
								strNodes[r] = strconv.Itoa(p)
								break
							}
						}
					}
					unwitnessedMessage = &template.UnwitnessedMessage{Step: s.step,
						Id:                   s.ServerIdentity(),
						SentArray:            convertInt2DtoString1D(s.sent, s.maxNodeCount, s.maxNodeCount),
						NodesProposal:        strNodes,
						RandomNumber:         randomNumber,
						ConsensusRoundNumber: s.maxConsensusRounds,
						Messagetype:          2,
						ConsensusStepNumber:  0,
						FoundConsensus:       false}
				}

				if stepNow > s.maxTime {
					return
				}
				value, ok := s.sentUnwitnessMessages[stepNow]

				if !ok {
					broadcastUnwitnessedMessage(s.admissionCommittee, s, unwitnessedMessage)
					s.sentUnwitnessMessages[stepNow] = unwitnessedMessage
				} else {
					fmt.Printf("Unwitnessed message %s for step %d from %s is already sent; possible race condition \n", value, stepNow, s.ServerIdentity())
				}

				unAckedUnwitnessedMessages := s.recievedTempUnwitnessedMessages[stepNow]
				for _, uauwm := range unAckedUnwitnessedMessages {
					s.recievedUnwitnessedMessages[uauwm.Step] = append(s.recievedUnwitnessedMessages[uauwm.Step], uauwm)
					newAck := &template.AcknowledgementMessage{Id: s.ServerIdentity(),
						UnwitnessedMessage: uauwm,
						SentArray:          convertInt2DtoString1D(s.sent, s.maxNodeCount, s.maxNodeCount)}
					requesterIdentity := uauwm.Id
					unicastAcknowledgementMessage(requesterIdentity, s, newAck)
					s.sentAcknowledgementMessages[uauwm.Step] = append(s.sentAcknowledgementMessages[uauwm.Step], newAck)
				}

			}

		}

	}
}

func handleJoinRequestMessage(s *Service, req *template.NodeJoinRequest) {

	newResponse := &template.NodeJoinResponse{Id: s.ServerIdentity()}
	requesterIdentity := req.Id
	unicastNodeResponseMessage(requesterIdentity, s, newResponse)

}

func handleJoinResponseMessage(s *Service, req *template.NodeJoinResponse) {
	s.receivedNodeJoinResponse = append(s.receivedNodeJoinResponse, req)
	if !s.receivedEnoughNodeResponses {
		if len(s.receivedNodeJoinResponse) > len(s.admissionCommittee)/2+1 {
			s.receivedEnoughNodeResponses = true
			newNodeJoinConfirmation := &template.NodeJoinConfirmation{Id: s.ServerIdentity()}
			broadcastNodeJoinConfirmationMessage(s.admissionCommittee, s, newNodeJoinConfirmation)
		}
	}
}

func handleJoinConfirmationMessage(s *Service, req *template.NodeJoinConfirmation) {
	s.newNodes = append(s.newNodes, req.Id)
}

func handleJoinAdmissionCommittee(s *Service, req *template.JoinAdmissionCommittee) {

	if !s.receivedAdmissionCommitteeJoin {
		s.receivedAdmissionCommitteeJoin = true
		fmt.Printf("%s joined the admission committee at step %d \n", s.ServerIdentity(), req.Step)
		s.step = req.Step
		s.admissionCommittee = make([]*network.ServerIdentity, 0)
		for t := 0; t < len(req.NewCommitee); t++ {
			y, _ := strconv.Atoi(req.NewCommitee[t])
			s.admissionCommittee = append(s.admissionCommittee, s.rosterNodes[y])
		}
		s.majority = len(s.admissionCommittee)/2 + 1

		s.sent = make([][]int, s.maxNodeCount)

		for i := 0; i < s.maxNodeCount; i++ {
			s.sent[i] = make([]int, s.maxNodeCount)
			for j := 0; j < s.maxNodeCount; j++ {
				s.sent[i][j] = 0
			}
		}

		s.deliv = make([]int, s.maxNodeCount)

		for j := 0; j < s.maxNodeCount; j++ {
			s.deliv[j] = 0
		}

		s.name = string(s.ServerIdentity().Address)

		s.vectorClockMemberList = make([]string, s.maxNodeCount)

		for i := 0; i < len(s.admissionCommittee); i++ {
			s.vectorClockMemberList[i] = string(s.admissionCommittee[i].Address)
		}
		for i := len(s.admissionCommittee); i < s.maxNodeCount; i++ {
			s.vectorClockMemberList[i] = ""
		}

		pingDistances := make([]int, len(s.admissionCommittee))
		for i := 0; i < len(s.admissionCommittee); i++ {
			pingDistances[i] = rand.Intn(300)
		}

		unwitnessedMessage := &template.UnwitnessedMessage{Step: s.step,
			Id:                       s.ServerIdentity(),
			SentArray:                convertInt2DtoString1D(s.sent, s.maxNodeCount, s.maxNodeCount),
			Messagetype:              1,
			PingDistances:            convertIntArraytoStringArray(pingDistances),
			PingMulticastRoundNumber: s.multiCastRounds}

		broadcastUnwitnessedMessage(s.admissionCommittee, s, unwitnessedMessage)
		s.sentUnwitnessMessages[s.step] = unwitnessedMessage

		s.stepLock.Unlock()

	}
}

func delayReception(s *Service, senderIndex int, receiverIndex int) {
	delay := s.nodeDelays[senderIndex][receiverIndex]
	time.Sleep(time.Duration(delay) * time.Millisecond)
	return
}

func newService(c *onet.Context) (onet.Service, error) {
	s := &Service{
		ServiceProcessor: onet.NewServiceProcessor(c),
		step:             0,
		stepLock:         new(sync.Mutex),

		maxTime: 50,
		active:  true,

		maxNodeCount: 30,

		maxConsensusRounds: 128,

		multiCastRounds: 10,

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

		recievedThresholdStepWitnessedMessages:     make(map[int][]*template.WitnessedMessage),
		recievedThresholdStepWitnessedMessagesLock: new(sync.Mutex),

		recievedAcksBool:     make(map[int]bool),
		recievedAcksBoolLock: new(sync.Mutex),

		recievedWitnessedMessagesBool:     make(map[int]bool),
		recievedWitnessedMessagesBoolLock: new(sync.Mutex),

		bufferedUnwitnessedMessages:     make([]*template.UnwitnessedMessage, 0),
		bufferedUnwitnessedMessagesLock: new(sync.Mutex),

		bufferedAckMessages:     make([]*template.AcknowledgementMessage, 0),
		bufferedAckMessagesLock: new(sync.Mutex),

		bufferedWitnessedMessages:     make([]*template.WitnessedMessage, 0),
		bufferedWitnessedMessagesLock: new(sync.Mutex),

		bufferedCatchupMessages:     make([]*template.CatchUpMessage, 0),
		bufferedCatchupMessagesLock: new(sync.Mutex),

		newNodes: make([]*network.ServerIdentity, 0),

		sentLock:  new(sync.Mutex),
		delivLock: new(sync.Mutex),

		tempNewCommittee: nil,

		receivedAdmissionCommitteeJoin: false,
	}
	if err := s.RegisterHandlers(s.SetGenesisSet, s.InitRequest, s.JoinRequest, s.SetRoster, s.SetActive); err != nil {
		return nil, errors.New("couldn't register messages")
	}

	s.RegisterProcessorFunc(unwitnessedMessageMsgID, func(arg1 *network.Envelope) error {

		defer s.stepLock.Unlock()
		s.stepLock.Lock()

		if s.active {

			req, ok := arg1.Msg.(*template.UnwitnessedMessage)
			if !ok {
				log.Error(s.ServerIdentity(), "failed to cast to unwitnessed message")
				return nil
			}

			myIndex := findIndexOf(s.vectorClockMemberList, s.name)
			senderIndex := findIndexOf(s.vectorClockMemberList, string(req.Id.Address))
			delayReception(s, senderIndex, myIndex)
			canDeleiver := true

			reqSentArray := convertString1DtoInt2D(req.SentArray, s.maxNodeCount, s.maxNodeCount)

			for i := 0; i < s.maxNodeCount; i++ {
				if s.deliv[i] < reqSentArray[i][myIndex] {
					canDeleiver = false
					break
				}
			}

			if canDeleiver {
				handleUnwitnessedMessage(s, req)

			} else {
				s.bufferedUnwitnessedMessages = append(s.bufferedUnwitnessedMessages, req)
			}

			handleBufferedMessages(s)
		}
		return nil
	})

	s.RegisterProcessorFunc(acknowledgementMessageMsgID, func(arg1 *network.Envelope) error {
		defer s.stepLock.Unlock()
		s.stepLock.Lock()
		if s.active {
			req, ok := arg1.Msg.(*template.AcknowledgementMessage)
			if !ok {
				log.Error(s.ServerIdentity(), "failed to cast to ack message")
				return nil
			}

			myIndex := findIndexOf(s.vectorClockMemberList, s.name)
			senderIndex := findIndexOf(s.vectorClockMemberList, string(req.Id.Address))
			delayReception(s, senderIndex, myIndex)

			canDeleiver := true

			reqSentArray := convertString1DtoInt2D(req.SentArray, s.maxNodeCount, s.maxNodeCount)

			for i := 0; i < s.maxNodeCount; i++ {
				if s.deliv[i] < reqSentArray[i][myIndex] {
					canDeleiver = false
					break
				}
			}

			if canDeleiver {
				handleAckMessage(s, req)

			} else {
				s.bufferedAckMessages = append(s.bufferedAckMessages, req)
			}

			handleBufferedMessages(s)
		}
		return nil
	})

	s.RegisterProcessorFunc(witnessedMessageMsgID, func(arg1 *network.Envelope) error {
		defer s.stepLock.Unlock()
		s.stepLock.Lock()

		if s.active {

			req, ok := arg1.Msg.(*template.WitnessedMessage)
			if !ok {
				log.Error(s.ServerIdentity(), "failed to cast to witnessed message")
				return nil
			}
			myIndex := findIndexOf(s.vectorClockMemberList, s.name)
			senderIndex := findIndexOf(s.vectorClockMemberList, string(req.Id.Address))
			delayReception(s, senderIndex, myIndex)

			canDeleiver := true

			reqSentArray := convertString1DtoInt2D(req.SentArray, s.maxNodeCount, s.maxNodeCount)

			for i := 0; i < s.maxNodeCount; i++ {
				if s.deliv[i] < reqSentArray[i][myIndex] {
					canDeleiver = false
					break
				}
			}

			if canDeleiver {
				handleWitnessedMessage(s, req)

			} else {
				s.bufferedWitnessedMessages = append(s.bufferedWitnessedMessages, req)
			}

			handleBufferedMessages(s)
		}
		return nil
	})

	s.RegisterProcessorFunc(nodeJoinRequestMessageMsgID, func(arg1 *network.Envelope) error {
		if s.active {
			req, ok := arg1.Msg.(*template.NodeJoinRequest)
			if !ok {
				log.Error(s.ServerIdentity(), "failed to cast to node join request message")
				return nil
			}
			handleJoinRequestMessage(s, req)
		}
		return nil
	})

	s.RegisterProcessorFunc(nodeJoinResponseMessageMsgID, func(arg1 *network.Envelope) error {
		if s.active {
			req, ok := arg1.Msg.(*template.NodeJoinResponse)
			if !ok {
				log.Error(s.ServerIdentity(), "failed to cast to node join response message")
				return nil
			}
			handleJoinResponseMessage(s, req)
		}
		return nil
	})

	s.RegisterProcessorFunc(nodeJoinConfirmationMessageMsgID, func(arg1 *network.Envelope) error {
		if s.active {
			req, ok := arg1.Msg.(*template.NodeJoinConfirmation)
			if !ok {
				log.Error(s.ServerIdentity(), "failed to cast to node join confirmation message")
				return nil
			}
			handleJoinConfirmationMessage(s, req)
		}
		return nil
	})

	s.RegisterProcessorFunc(nodeJoinAdmissionCommitteeMessageMsgID, func(arg1 *network.Envelope) error {
		if s.active {
			req, ok := arg1.Msg.(*template.JoinAdmissionCommittee)
			if !ok {
				log.Error(s.ServerIdentity(), "failed to cast to node join admission committee message")
				return nil
			}
			handleJoinAdmissionCommittee(s, req)
		}
		return nil
	})

	return s, nil
}

func handleBufferedMessages(s *Service) {

	if len(s.bufferedUnwitnessedMessages) > 0 {

		myIndex := findIndexOf(s.vectorClockMemberList, s.name)

		processedBufferedMessages := make([]int, 0)
		for k := 0; k < len(s.bufferedUnwitnessedMessages); k++ {

			bufferedRequest := s.bufferedUnwitnessedMessages[k]

			if bufferedRequest == nil {
				continue
			}

			canDeleiver := true
			reqSentArray := convertString1DtoInt2D(bufferedRequest.SentArray, s.maxNodeCount, s.maxNodeCount)

			for i := 0; i < s.maxNodeCount; i++ {
				if s.deliv[i] < reqSentArray[i][myIndex] {
					canDeleiver = false
					break
				}
			}

			if canDeleiver {
				processedBufferedMessages = append(processedBufferedMessages, k)
				handleUnwitnessedMessage(s, bufferedRequest)
			}
		}
		for q := 0; q < len(processedBufferedMessages); q++ {
			s.bufferedUnwitnessedMessages[processedBufferedMessages[q]] = nil
		}

	}

	if len(s.bufferedAckMessages) > 0 {

		myIndex := findIndexOf(s.vectorClockMemberList, s.name)

		processedBufferedMessages := make([]int, 0)
		for k := 0; k < len(s.bufferedAckMessages); k++ {

			bufferedRequest := s.bufferedAckMessages[k]

			if bufferedRequest == nil {
				continue
			}

			canDeleiver := true
			reqSentArray := convertString1DtoInt2D(bufferedRequest.SentArray, s.maxNodeCount, s.maxNodeCount)

			for i := 0; i < s.maxNodeCount; i++ {
				if s.deliv[i] < reqSentArray[i][myIndex] {
					canDeleiver = false
					break
				}
			}

			if canDeleiver {
				processedBufferedMessages = append(processedBufferedMessages, k)
				handleAckMessage(s, bufferedRequest)
			}
		}
		for q := 0; q < len(processedBufferedMessages); q++ {
			s.bufferedAckMessages[processedBufferedMessages[q]] = nil
		}

	}

	if len(s.bufferedWitnessedMessages) > 0 {

		myIndex := findIndexOf(s.vectorClockMemberList, s.name)

		processedBufferedMessages := make([]int, 0)

		for k := 0; k < len(s.bufferedWitnessedMessages); k++ {

			bufferedRequest := s.bufferedWitnessedMessages[k]

			if bufferedRequest == nil {
				continue
			}

			canDeleiver := true
			reqSentArray := convertString1DtoInt2D(bufferedRequest.SentArray, s.maxNodeCount, s.maxNodeCount)

			for i := 0; i < s.maxNodeCount; i++ {
				if s.deliv[i] < reqSentArray[i][myIndex] {
					canDeleiver = false
					break
				}
			}

			if canDeleiver {
				processedBufferedMessages = append(processedBufferedMessages, k)
				handleWitnessedMessage(s, bufferedRequest)
			}
		}
		for q := 0; q < len(processedBufferedMessages); q++ {
			s.bufferedWitnessedMessages[processedBufferedMessages[q]] = nil
		}

	}

}
