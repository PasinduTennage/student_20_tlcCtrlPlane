package service

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/dedis/cothority_template"
	"go.dedis.ch/onet/v3"
	"go.dedis.ch/onet/v3/log"
	"go.dedis.ch/onet/v3/network"
	"math/rand"
	"strconv"
	"sync"
)

var unwitnessedMessageMsgID network.MessageTypeID
var witnessedMessageMsgID network.MessageTypeID
var acknowledgementMessageMsgID network.MessageTypeID
var catchUpMessageID network.MessageTypeID

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

type Service struct {
	// We need to embed the ServiceProcessor, so that incoming messages
	// are correctly handled.
	*onet.ServiceProcessor

	storage *storage

	tempConsensusNodes []*network.ServerIdentity

	tempPingConsensus []string

	pingConsensus [][]int

	admissionCommittee []*network.ServerIdentity
	tempNewCommittee   []*network.ServerIdentity

	step     int
	stepLock *sync.Mutex

	name                  string
	vectorClockMemberList []string

	maxNodeCount int

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

func (s *Service) InitRequest(req *template.InitRequest) (*template.InitResponse, error) {

	defer s.stepLock.Unlock()
	s.stepLock.Lock()

	unwitnessedMessage := &template.UnwitnessedMessage{Step: s.step, Id: s.ServerIdentity(), SentArray: convertInt2DtoString1D(s.sent, s.maxNodeCount, s.maxNodeCount), Messagetype: 0}

	broadcastUnwitnessedMessage(s.admissionCommittee, s, unwitnessedMessage)

	s.sentUnwitnessMessages[s.step] = unwitnessedMessage // check syncMaps in go

	return &template.InitResponse{}, nil

}

func (s *Service) SetGenesisSet(req *template.GenesisNodesRequest) (*template.GenesisNodesResponse, error) {

	defer s.stepLock.Unlock()
	s.stepLock.Lock()

	s.admissionCommittee = convertStringArraytoNetworkId(req.Nodes)

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

func convertNetworkIdtoStringArray(nodeIds []*network.ServerIdentity) []string {
	stringArray := make([]string, len(nodeIds))
	for i := 0; i < len(nodeIds); i++ {
		jsonNodeId, _ := json.Marshal(nodeIds[i])
		stringArray[i] = string(jsonNodeId)
	}
	return stringArray
}

func convertStringArraytoNetworkId(array []string) []*network.ServerIdentity {
	IdArray := make([]*network.ServerIdentity, len(array))
	for i := 0; i < len(array); i++ {
		byteArray := []byte(array[i])
		var m network.ServerIdentity
		json.Unmarshal(byteArray, &m)
		IdArray[i] = &m
	}

	return IdArray
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
		stringArray[i] = string(values[i])
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

	s.deliv[reqIndex] = s.deliv[reqIndex] + 1

	stepNow := s.step

	if req.Step <= stepNow {

		s.recievedUnwitnessedMessages[req.Step] = append(s.recievedUnwitnessedMessages[req.Step], req)
		newAck := &template.AcknowledgementMessage{Id: s.ServerIdentity(), UnwitnessedMessage: req, SentArray: convertInt2DtoString1D(s.sent, s.maxNodeCount, s.maxNodeCount)}
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
				ackIdJson, _ := json.Marshal(ackId)
				for _, num := range nodes {
					numJson, _ := json.Marshal(num)

					if string(numJson) == string(ackIdJson) {
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
					newWitness = &template.WitnessedMessage{Step: stepNow, Id: s.ServerIdentity(), SentArray: convertInt2DtoString1D(s.sent, s.maxNodeCount, s.maxNodeCount), Messagetype: 0}
				}
				if s.sentUnwitnessMessages[stepNow].Messagetype == 1 {
					newWitness = &template.WitnessedMessage{Step: stepNow, Id: s.ServerIdentity(), SentArray: convertInt2DtoString1D(s.sent, s.maxNodeCount, s.maxNodeCount), Messagetype: 1, PingDistances: s.sentUnwitnessMessages[stepNow].PingDistances}
				}
				if s.sentUnwitnessMessages[stepNow].Messagetype == 2 {
					if s.sentUnwitnessMessages[stepNow].ConsensusStepNumber == 0 {
						newWitness = &template.WitnessedMessage{Step: stepNow, Id: s.ServerIdentity(), SentArray: convertInt2DtoString1D(s.sent, s.maxNodeCount, s.maxNodeCount), RandomNumber: s.sentUnwitnessMessages[s.step].RandomNumber, NodesProposal: s.sentUnwitnessMessages[s.step].NodesProposal, ConsensusRoundNumber: s.sentUnwitnessMessages[s.step].ConsensusRoundNumber, Messagetype: 2, ConsensusStepNumber: s.sentUnwitnessMessages[s.step].ConsensusStepNumber}
					}
					if s.sentUnwitnessMessages[stepNow].ConsensusStepNumber == 1 {
						newWitness = &template.WitnessedMessage{Step: stepNow, Id: s.ServerIdentity(), SentArray: convertInt2DtoString1D(s.sent, s.maxNodeCount, s.maxNodeCount), Nodes: s.sentUnwitnessMessages[stepNow].Nodes, ConsensusRoundNumber: s.sentUnwitnessMessages[s.step].ConsensusRoundNumber, Messagetype: 2, ConsensusStepNumber: s.sentUnwitnessMessages[s.step].ConsensusStepNumber}
					}
					if s.sentUnwitnessMessages[stepNow].ConsensusStepNumber == 2 {
						newWitness = &template.WitnessedMessage{Step: stepNow, Id: s.ServerIdentity(), SentArray: convertInt2DtoString1D(s.sent, s.maxNodeCount, s.maxNodeCount), ConsensusRoundNumber: s.sentUnwitnessMessages[s.step].ConsensusRoundNumber, Messagetype: 2, ConsensusStepNumber: s.sentUnwitnessMessages[s.step].ConsensusStepNumber}
					}
				}
				if s.sentUnwitnessMessages[stepNow].Messagetype == 3 {
					if s.sentUnwitnessMessages[stepNow].ConsensusStepNumber == 0 {
						newWitness = &template.WitnessedMessage{Step: stepNow, Id: s.ServerIdentity(), SentArray: convertInt2DtoString1D(s.sent, s.maxNodeCount, s.maxNodeCount), RandomNumber: s.sentUnwitnessMessages[s.step].RandomNumber, PingMetrix: s.sentUnwitnessMessages[s.step].PingMetrix, ConsensusRoundNumber: s.sentUnwitnessMessages[s.step].ConsensusRoundNumber, Messagetype: 3, ConsensusStepNumber: s.sentUnwitnessMessages[s.step].ConsensusStepNumber}
					}
					if s.sentUnwitnessMessages[stepNow].ConsensusStepNumber == 1 {
						newWitness = &template.WitnessedMessage{Step: stepNow, Id: s.ServerIdentity(), SentArray: convertInt2DtoString1D(s.sent, s.maxNodeCount, s.maxNodeCount), Nodes: s.sentUnwitnessMessages[stepNow].Nodes, ConsensusRoundNumber: s.sentUnwitnessMessages[s.step].ConsensusRoundNumber, Messagetype: 3, ConsensusStepNumber: s.sentUnwitnessMessages[s.step].ConsensusStepNumber}
					}
					if s.sentUnwitnessMessages[stepNow].ConsensusStepNumber == 2 {
						newWitness = &template.WitnessedMessage{Step: stepNow, Id: s.ServerIdentity(), SentArray: convertInt2DtoString1D(s.sent, s.maxNodeCount, s.maxNodeCount), ConsensusRoundNumber: s.sentUnwitnessMessages[s.step].ConsensusRoundNumber, Messagetype: 3, ConsensusStepNumber: s.sentUnwitnessMessages[s.step].ConsensusStepNumber}
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
				twmIdJson, _ := json.Marshal(twmId)
				exists := false
				for _, num := range nodes {
					numJson, _ := json.Marshal(num)
					if string(numJson) == string(twmIdJson) {
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
					jsnNodeId, _ := json.Marshal(nodeId)
					for _, twm := range s.recievedThresholdwitnessedMessages[stepNow] {
						jsnTwmId, _ := json.Marshal(twm.Id)
						if string(jsnNodeId) == string(jsnTwmId) {
							s.recievedThresholdStepWitnessedMessages[stepNow] = append(s.recievedThresholdStepWitnessedMessages[stepNow], twm)
							break
						}
					}
				}

				s.step = s.step + 1
				if s.step == 1 {
					s.majority = len(s.admissionCommittee)/2 + 1
				}
				stepNow = s.step

				fmt.Printf("%s increased time step to %d \n", s.ServerIdentity(), stepNow)

				var unwitnessedMessage *template.UnwitnessedMessage

				if stepNow == 1 {

					s.tempNewCommittee = s.admissionCommittee
					nodes := s.tempNewCommittee

					strNodes := convertNetworkIdtoStringArray(nodes)

					randomNumber := rand.Intn(10000)

					fmt.Printf("%s's initial proposal random number is %d \n", s.ServerIdentity(), randomNumber)

					unwitnessedMessage = &template.UnwitnessedMessage{Step: s.step, Id: s.ServerIdentity(), SentArray: convertInt2DtoString1D(s.sent, s.maxNodeCount, s.maxNodeCount), NodesProposal: strNodes, RandomNumber: randomNumber, ConsensusRoundNumber: 10, Messagetype: 2, ConsensusStepNumber: 0}
				} // start member ship consensus

				if 1 < stepNow && stepNow <= 31 {
					if s.sentUnwitnessMessages[stepNow-1].ConsensusStepNumber == 0 {
						unwitnessedMessage = &template.UnwitnessedMessage{Step: stepNow, Id: s.ServerIdentity(), SentArray: convertInt2DtoString1D(s.sent, s.maxNodeCount, s.maxNodeCount), Nodes: convertNetworkIdtoStringArray(nodes), ConsensusRoundNumber: s.sentUnwitnessMessages[stepNow-1].ConsensusRoundNumber, Messagetype: 2, ConsensusStepNumber: 1}
					}
					if s.sentUnwitnessMessages[stepNow-1].ConsensusStepNumber == 1 {
						unwitnessedMessage = &template.UnwitnessedMessage{Step: stepNow, Id: s.ServerIdentity(), SentArray: convertInt2DtoString1D(s.sent, s.maxNodeCount, s.maxNodeCount), ConsensusRoundNumber: s.sentUnwitnessMessages[stepNow-1].ConsensusRoundNumber, Messagetype: 2, ConsensusStepNumber: 2}
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

							for j := 0; j < len(s.recievedThresholdwitnessedMessages[stepNow-3]); j++ {

								jsnTwmId, _ := json.Marshal(s.recievedThresholdwitnessedMessages[stepNow-3][j].Id)

								if CelibrityNodes[i] == string(jsnTwmId) {

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

								for j := 0; j < len(s.recievedThresholdwitnessedMessages[stepNow-3]); j++ {

									jsnTwmId, _ := json.Marshal(s.recievedThresholdwitnessedMessages[stepNow-3][j].Id)

									if CelibrityNodes1[i] == string(jsnTwmId) {

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
							s.tempConsensusNodes = convertStringArraytoNetworkId(consensusValue)
						} else {
							fmt.Printf("Did not find consensus\n")
						}

						if s.sentUnwitnessMessages[stepNow-1].ConsensusRoundNumber > 1 {

							strNodes := make([]string, 0)

							if consensusFound {
								strNodes = consensusValue
							} else if s.tempConsensusNodes != nil && len(s.tempConsensusNodes) > 0 {
								strNodes = convertNetworkIdtoStringArray(s.tempConsensusNodes)
							} else {

								nodes := s.tempNewCommittee

								strNodes = convertNetworkIdtoStringArray(nodes)
							}
							randomNumber := rand.Intn(10000)

							fmt.Printf("%s's new proposal random number is %d \n", s.ServerIdentity(), randomNumber)

							unwitnessedMessage = &template.UnwitnessedMessage{Step: stepNow, Id: s.ServerIdentity(), SentArray: convertInt2DtoString1D(s.sent, s.maxNodeCount, s.maxNodeCount), NodesProposal: strNodes, RandomNumber: randomNumber, ConsensusRoundNumber: s.sentUnwitnessMessages[stepNow-1].ConsensusRoundNumber - 1, Messagetype: 2, ConsensusStepNumber: 0}

						} else {
							// end of consensus rounds

							if s.tempConsensusNodes != nil && stepNow == 31 {

								fmt.Printf("%s Updated the roster with new set of nodes\n", s.ServerIdentity())

								s.admissionCommittee = s.tempConsensusNodes

								s.majority = len(s.admissionCommittee)/2 + 1

								s.name = string(s.ServerIdentity().Address)

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
									}

								}

								// calculate ping distances, for now lets mock the ping distances

								pingDistances := make([]int, len(s.admissionCommittee))
								for i := 0; i < len(s.admissionCommittee); i++ {
									pingDistances[i] = rand.Intn(300)
								}

								unwitnessedMessage = &template.UnwitnessedMessage{Step: stepNow, Id: s.ServerIdentity(), SentArray: convertInt2DtoString1D(s.sent, s.maxNodeCount, s.maxNodeCount), Messagetype: 1, PingDistances: convertIntArraytoStringArray(pingDistances)}

								s.tempConsensusNodes = nil

							}

						}

					}
				} // nodes consensus and when 31, send ping message

				if stepNow == 32 {
					unwitnessedMessage = &template.UnwitnessedMessage{Step: stepNow, Id: s.ServerIdentity(), SentArray: convertInt2DtoString1D(s.sent, s.maxNodeCount, s.maxNodeCount), Messagetype: 0}
				} // send normal message

				if stepNow == 33 {

					pingMatrix := make([][]int, len(s.admissionCommittee))

					for i := 0; i < len(s.admissionCommittee); i++ {
						for l := 0; l < len(s.recievedThresholdwitnessedMessages[31]); l++ {
							if string(s.recievedThresholdwitnessedMessages[31][l].Id.Address) == string(s.admissionCommittee[i].Address) {
								pingMatrix[i] = convertStringArraytoIntArray(s.recievedThresholdwitnessedMessages[31][l].PingDistances)
								break
							}
						}
					}

					pingMatrixStr := convertInt2DtoString1D(pingMatrix, len(s.admissionCommittee), len(s.admissionCommittee))

					randomNumber := rand.Intn(10000)

					fmt.Printf("%s's initial ping proposal random number is %d \n", s.ServerIdentity(), randomNumber)

					unwitnessedMessage = &template.UnwitnessedMessage{Step: s.step, Id: s.ServerIdentity(), SentArray: convertInt2DtoString1D(s.sent, s.maxNodeCount, s.maxNodeCount), PingMetrix: pingMatrixStr, RandomNumber: randomNumber, ConsensusRoundNumber: 10, Messagetype: 3, ConsensusStepNumber: 0}

				} // make ping distances matrx and start consensus

				if 33 < stepNow && stepNow <= 63 {
					if s.sentUnwitnessMessages[stepNow-1].ConsensusStepNumber == 0 {
						unwitnessedMessage = &template.UnwitnessedMessage{Step: stepNow, Id: s.ServerIdentity(), SentArray: convertInt2DtoString1D(s.sent, s.maxNodeCount, s.maxNodeCount), Nodes: convertNetworkIdtoStringArray(nodes), ConsensusRoundNumber: s.sentUnwitnessMessages[stepNow-1].ConsensusRoundNumber, Messagetype: 3, ConsensusStepNumber: 1}
					}
					if s.sentUnwitnessMessages[stepNow-1].ConsensusStepNumber == 1 {
						unwitnessedMessage = &template.UnwitnessedMessage{Step: stepNow, Id: s.ServerIdentity(), SentArray: convertInt2DtoString1D(s.sent, s.maxNodeCount, s.maxNodeCount), ConsensusRoundNumber: s.sentUnwitnessMessages[stepNow-1].ConsensusRoundNumber, Messagetype: 3, ConsensusStepNumber: 2}
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

							for j := 0; j < len(s.recievedThresholdwitnessedMessages[stepNow-3]); j++ {

								jsnTwmId, _ := json.Marshal(s.recievedThresholdwitnessedMessages[stepNow-3][j].Id)

								if CelibrityNodes[i] == string(jsnTwmId) {

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

								for j := 0; j < len(s.recievedThresholdwitnessedMessages[stepNow-3]); j++ {

									jsnTwmId, _ := json.Marshal(s.recievedThresholdwitnessedMessages[stepNow-3][j].Id)

									if CelibrityNodes1[i] == string(jsnTwmId) {

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
							} else {
								strPingMtrx = s.sentUnwitnessMessages[31].PingDistances
							}
							randomNumber := rand.Intn(10000)

							fmt.Printf("%s's new proposal random number is %d \n", s.ServerIdentity(), randomNumber)

							unwitnessedMessage = &template.UnwitnessedMessage{Step: stepNow, Id: s.ServerIdentity(), SentArray: convertInt2DtoString1D(s.sent, s.maxNodeCount, s.maxNodeCount), PingMetrix: strPingMtrx, RandomNumber: randomNumber, ConsensusRoundNumber: s.sentUnwitnessMessages[stepNow-1].ConsensusRoundNumber - 1, Messagetype: 3, ConsensusStepNumber: 0}

						} else {
							// end of consensus rounds

							if s.tempPingConsensus != nil && stepNow == 63 {

								s.pingConsensus = convertString1DtoInt2D(s.tempPingConsensus, len(s.admissionCommittee), len(s.admissionCommittee))

								unwitnessedMessage = &template.UnwitnessedMessage{Step: stepNow, Id: s.ServerIdentity(), SentArray: convertInt2DtoString1D(s.sent, s.maxNodeCount, s.maxNodeCount), Messagetype: 0}

								s.tempConsensusNodes = nil

							}

						}

					}

				} // ping matrix consensus

				if stepNow > 63 {
					unwitnessedMessage = &template.UnwitnessedMessage{Step: stepNow, Id: s.ServerIdentity(), SentArray: convertInt2DtoString1D(s.sent, s.maxNodeCount, s.maxNodeCount), Messagetype: 0}
				} // tlc messages

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
					newAck := &template.AcknowledgementMessage{Id: s.ServerIdentity(), UnwitnessedMessage: uauwm, SentArray: convertInt2DtoString1D(s.sent, s.maxNodeCount, s.maxNodeCount)}
					requesterIdentity := uauwm.Id
					unicastAcknowledgementMessage(requesterIdentity, s, newAck)
					s.sentAcknowledgementMessages[uauwm.Step] = append(s.sentAcknowledgementMessages[uauwm.Step], newAck)
				}

			}

		}

	}
}

func newService(c *onet.Context) (onet.Service, error) {
	s := &Service{
		ServiceProcessor: onet.NewServiceProcessor(c),

		step:     0,
		stepLock: new(sync.Mutex),

		maxTime: 200,

		maxNodeCount: 16,

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

		sentLock:  new(sync.Mutex),
		delivLock: new(sync.Mutex),

		tempConsensusNodes: nil,
	}
	if err := s.RegisterHandlers(s.SetGenesisSet, s.InitRequest); err != nil {
		return nil, errors.New("couldn't register messages")
	}

	s.RegisterProcessorFunc(unwitnessedMessageMsgID, func(arg1 *network.Envelope) error {

		defer s.stepLock.Unlock()
		s.stepLock.Lock()

		req, ok := arg1.Msg.(*template.UnwitnessedMessage)
		if !ok {
			log.Error(s.ServerIdentity(), "failed to cast to unwitnessed message")
			return nil
		}

		myIndex := findIndexOf(s.vectorClockMemberList, s.name)

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

		return nil
	})

	s.RegisterProcessorFunc(acknowledgementMessageMsgID, func(arg1 *network.Envelope) error {
		defer s.stepLock.Unlock()
		s.stepLock.Lock()

		req, ok := arg1.Msg.(*template.AcknowledgementMessage)
		if !ok {
			log.Error(s.ServerIdentity(), "failed to cast to ack message")
			return nil
		}

		myIndex := findIndexOf(s.vectorClockMemberList, s.name)

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
		return nil
	})

	s.RegisterProcessorFunc(witnessedMessageMsgID, func(arg1 *network.Envelope) error {
		defer s.stepLock.Unlock()
		s.stepLock.Lock()

		req, ok := arg1.Msg.(*template.WitnessedMessage)
		if !ok {
			log.Error(s.ServerIdentity(), "failed to cast to witnessed message")
			return nil
		}
		myIndex := findIndexOf(s.vectorClockMemberList, s.name)

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
