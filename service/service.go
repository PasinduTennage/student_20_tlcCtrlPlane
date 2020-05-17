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
	"math/rand"
	"strconv"
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
	roster             *onet.Roster
	storage            *storage
	tempConsensusNodes []*network.ServerIdentity

	step     int
	stepLock *sync.Mutex

	name        string
	memberNames []string

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

// storageID reflects the data we're storing - we could store more
// than one structure.
var storageID = []byte("main")

// storage is used to save our data.
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

		senderIndex := findIndexOf(s.memberNames, s.name)
		receiverIndex := findIndexOf(s.memberNames, string(node.Address))

		s.sent[senderIndex][receiverIndex] = s.sent[senderIndex][receiverIndex] + 1

		if e != nil {
			panic(e)
		}
	}
}

func broadcastWitnessedMessage(memberNodes []*network.ServerIdentity, s *Service, message *template.WitnessedMessage) {
	for _, node := range memberNodes {
		e := s.SendRaw(node, message)

		senderIndex := findIndexOf(s.memberNames, s.name)
		receiverIndex := findIndexOf(s.memberNames, string(node.Address))

		s.sent[senderIndex][receiverIndex] = s.sent[senderIndex][receiverIndex] + 1

		if e != nil {
			panic(e)
		}
	}
}

func unicastAcknowledgementMessage(memberNode *network.ServerIdentity, s *Service, message *template.AcknowledgementMessage) {
	e := s.SendRaw(memberNode, message)

	senderIndex := findIndexOf(s.memberNames, s.name)
	receiverIndex := findIndexOf(s.memberNames, string(memberNode.Address))

	s.sent[senderIndex][receiverIndex] = s.sent[senderIndex][receiverIndex] + 1

	if e != nil {
		panic(e)
	}

}

//func unicastCatchUpMessage(memberNode *network.ServerIdentity, s *Service, message *template.CatchUpMessage) {
//e := s.SendRaw(memberNode, message)
//
//	senderIndex := findIndexOf(s.memberNames, s.name)
//receiverIndex := findIndexOf(s.memberNames, string(memberNode.Address))
//

//	s.sent[senderIndex][receiverIndex] = s.sent[senderIndex][receiverIndex] + 1
//
//	if e != nil {
//		panic(e)
//	}
//
//}

func (s *Service) InitRequest(req *template.InitRequest) (*template.InitResponse, error) {

	defer s.stepLock.Unlock()
	s.stepLock.Lock()

	s.roster = req.SsRoster
	//fmt.Printf("Roster is set for %s", s.ServerIdentity())

	s.majority = len(s.roster.List)/2 + 1

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

	time.Sleep(2 * time.Second)

	stepNow := s.step

	s.name = string(s.ServerIdentity().Address)

	s.memberNames = make([]string, s.maxNodeCount)

	for i := 0; i < len(s.roster.List); i++ {
		s.memberNames[i] = string(s.roster.List[i].Address)
	}
	for i := len(s.roster.List); i < s.maxNodeCount; i++ {
		s.memberNames[i] = ""
	}

	nodes := make([]*network.ServerIdentity, 0)

	for _, node := range s.roster.List {
		nodes = append(nodes, node)

	}

	strNodes := convertNetworkIdtoStringArray(nodes)

	randomNumber := rand.Intn(10000)

	fmt.Printf("%s's initial proposal random number is %d \n", s.ServerIdentity(), randomNumber)

	unwitnessedMessage := &template.UnwitnessedMessage{Step: stepNow, Id: s.ServerIdentity(), SentArray: convertInt2DtoString1D(s.sent, s.maxNodeCount, s.maxNodeCount), NodesProposal: strNodes, RandomNumber: randomNumber, ConsensusRoundNumber: 30, IsConsensus: true, ConsensusStepNumber: 0}
	//unwitnessedMessage := &template.UnwitnessedMessage{Step: stepNow, Id: s.ServerIdentity(), SentArray: convertInt2DtoString1D(s.sent, len(s.roster.List), len(s.roster.List)), IsConsensus: false}

	broadcastUnwitnessedMessage(s.roster.List, s, unwitnessedMessage)

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

func findGlobalMaxNumberOfNodes(messages []*template.WitnessedMessage) int {
	globalMax := 0
	for i := 0; i < len(messages); i++ {
		if len(messages[i].NodesProposal) > globalMax {
			globalMax = len(messages[i].NodesProposal)
		}
	}
	return globalMax
}

func findValueWithMaxKey(rowMap map[int][]string) (int, []string) {
	maxKey := 0
	for n := range rowMap {
		if n > maxKey {
			maxKey = n
		}
	}
	return maxKey, rowMap[maxKey]
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

func handleUnwitnessedMessage(s *Service, req *template.UnwitnessedMessage) {

	reqSentArray := convertString1DtoInt2D(req.SentArray, s.maxNodeCount, s.maxNodeCount)

	for i := 0; i < s.maxNodeCount; i++ {
		for j := 0; j < s.maxNodeCount; j++ {
			s.sent[i][j] = max(s.sent[i][j], reqSentArray[i][j])
		}
	}

	reqIndex := findIndexOf(s.memberNames, string(req.Id.Address))

	s.deliv[reqIndex] = s.deliv[reqIndex] + 1

	//fmt.Printf("%s at %d received unwitnessed from %s with step %d \n", s.ServerIdentity(), s.step, req.Id, req.Step)

	stepNow := s.step

	if req.Step <= stepNow {

		s.recievedUnwitnessedMessages[req.Step] = append(s.recievedUnwitnessedMessages[req.Step], req)
		newAck := &template.AcknowledgementMessage{Id: s.ServerIdentity(), UnwitnessedMessage: req, SentArray: convertInt2DtoString1D(s.sent, s.maxNodeCount, s.maxNodeCount)}
		requesterIdentity := req.Id
		unicastAcknowledgementMessage(requesterIdentity, s, newAck)
		s.sentAcknowledgementMessages[req.Step] = append(s.sentAcknowledgementMessages[req.Step], newAck)
		//fmt.Printf("%s at %d sent ack to %s with step %d \n", s.ServerIdentity(), s.step, req.Id, newAck.UnwitnessedMessage.Step)

		//}
		//else if req.Step < stepNow {
		//
		//	s.recievedUnwitnessedMessages[req.Step] = append(s.recievedUnwitnessedMessages[req.Step], req)
		//	newAck := &template.AcknowledgementMessage{Id: s.ServerIdentity(), UnwitnessedMessage: req, SentArray: convertInt2DtoString1D(s.sent, s)}
		//	requesterIdentity := req.Id
		//	unicastAcknowledgementMessage(requesterIdentity, s, newAck)
		//	s.sentAcknowledgementMessages[req.Step] = append(s.sentAcknowledgementMessages[req.Step], newAck)
		//fmt.Printf("%s at %d sent ack to %s with step %d \n", s.ServerIdentity(), s.step, req.Id, newAck.UnwitnessedMessage.Step)

		//catchUpThresholdwitnessedMessages := make(map[int]*template.ArrayWitnessedMessages)
		//for i := req.Step; i < stepNow; i++ {
		//
		//	recievedMessages := s.recievedThresholdwitnessedMessages[i]
		//	newArrayWitnessedMessages := &template.ArrayWitnessedMessages{Messages: recievedMessages}
		//	catchUpThresholdwitnessedMessages[i] = newArrayWitnessedMessages
		//}
		//
		//newCatchUpMessage := &template.CatchUpMessage{Id: s.ServerIdentity(), Step: stepNow, RecievedThresholdwitnessedMessages: catchUpThresholdwitnessedMessages, SentArray: convertInt2DtoString1D(s.sent, s)}
		//unicastCatchUpMessage(req.Id, s, newCatchUpMessage)
		//fmt.Printf("%s at %d sent catchup to %s with step %d \n", s.ServerIdentity(), s.step, req.Id, stepNow)

	} else if req.Step > stepNow {
		// save the unwitnessed message and later send ack
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

	reqIndex := findIndexOf(s.memberNames, string(req.Id.Address))

	s.deliv[reqIndex] = s.deliv[reqIndex] + 1

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
				ackId := ack.Id
				exists := false
				for _, num := range nodes {
					numJson, _ := json.Marshal(num)
					ackIdJson, _ := json.Marshal(ackId)

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
				//fmt.Printf("%s Recieved a majority of Acks \n", s.ServerIdentity())
				s.recievedAcksBool[stepNow] = true

				var newWitness *template.WitnessedMessage
				if s.sentUnwitnessMessages[stepNow].IsConsensus == true {

					if s.sentUnwitnessMessages[stepNow].ConsensusStepNumber == 0 {
						newWitness = &template.WitnessedMessage{Step: stepNow, Id: s.ServerIdentity(), SentArray: convertInt2DtoString1D(s.sent, s.maxNodeCount, s.maxNodeCount), RandomNumber: s.sentUnwitnessMessages[s.step].RandomNumber, NodesProposal: s.sentUnwitnessMessages[s.step].NodesProposal, ConsensusRoundNumber: s.sentUnwitnessMessages[s.step].ConsensusRoundNumber, IsConsensus: true, ConsensusStepNumber: s.sentUnwitnessMessages[s.step].ConsensusStepNumber}
					}
					if s.sentUnwitnessMessages[stepNow].ConsensusStepNumber == 1 {
						newWitness = &template.WitnessedMessage{Step: stepNow, Id: s.ServerIdentity(), SentArray: convertInt2DtoString1D(s.sent, s.maxNodeCount, s.maxNodeCount), Nodes: s.sentUnwitnessMessages[stepNow].Nodes, ConsensusRoundNumber: s.sentUnwitnessMessages[s.step].ConsensusRoundNumber, IsConsensus: true, ConsensusStepNumber: s.sentUnwitnessMessages[s.step].ConsensusStepNumber}
					}
					if s.sentUnwitnessMessages[stepNow].ConsensusStepNumber == 2 {
						newWitness = &template.WitnessedMessage{Step: stepNow, Id: s.ServerIdentity(), SentArray: convertInt2DtoString1D(s.sent, s.maxNodeCount, s.maxNodeCount), ConsensusRoundNumber: s.sentUnwitnessMessages[s.step].ConsensusRoundNumber, IsConsensus: true, ConsensusStepNumber: s.sentUnwitnessMessages[s.step].ConsensusStepNumber}
					}

				} else {
					newWitness = &template.WitnessedMessage{Step: stepNow, Id: s.ServerIdentity(), SentArray: convertInt2DtoString1D(s.sent, s.maxNodeCount, s.maxNodeCount), IsConsensus: false}
				}

				broadcastWitnessedMessage(s.roster.List, s, newWitness)
				s.sentThresholdWitnessedMessages[stepNow] = newWitness
				//fmt.Printf("%s at %d broadcast witnessed with step %d \n", s.ServerIdentity(), s.step, newWitness.Step)
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

	reqIndex := findIndexOf(s.memberNames, string(req.Id.Address))

	s.deliv[reqIndex] = s.deliv[reqIndex] + 1

	//fmt.Printf("%s at %d received witnessed from %s with step %d \n", s.ServerIdentity(), s.step, req.Id, req.Step)

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
					numJson, _ := json.Marshal(num)
					twmIdJson, _ := json.Marshal(twmId)

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
					for _, twm := range s.recievedThresholdwitnessedMessages[stepNow] {
						jsnNodeId, _ := json.Marshal(nodeId)
						jsnTwmId, _ := json.Marshal(twm.Id)

						if string(jsnNodeId) == string(jsnTwmId) {
							s.recievedThresholdStepWitnessedMessages[stepNow] = append(s.recievedThresholdStepWitnessedMessages[stepNow], twm)
							break
						}
					}
				}

				//fmt.Printf("The step threshold witnessed message array is %s for %d \n", s.recievedThresholdStepWitnessedMessages[stepNow], stepNow)

				s.step = s.step + 1
				stepNow = s.step

				fmt.Printf("%s increased time step to %d \n", s.ServerIdentity(), stepNow)

				var unwitnessedMessage *template.UnwitnessedMessage

				if s.sentUnwitnessMessages[stepNow-1].IsConsensus == true {

					if s.sentUnwitnessMessages[stepNow-1].ConsensusStepNumber == 0 {
						unwitnessedMessage = &template.UnwitnessedMessage{Step: stepNow, Id: s.ServerIdentity(), SentArray: convertInt2DtoString1D(s.sent, s.maxNodeCount, s.maxNodeCount), Nodes: convertNetworkIdtoStringArray(nodes), ConsensusRoundNumber: s.sentUnwitnessMessages[stepNow-1].ConsensusRoundNumber, IsConsensus: true, ConsensusStepNumber: 1}
					}
					if s.sentUnwitnessMessages[stepNow-1].ConsensusStepNumber == 1 {
						unwitnessedMessage = &template.UnwitnessedMessage{Step: stepNow, Id: s.ServerIdentity(), SentArray: convertInt2DtoString1D(s.sent, s.maxNodeCount, s.maxNodeCount), ConsensusRoundNumber: s.sentUnwitnessMessages[stepNow-1].ConsensusRoundNumber, IsConsensus: true, ConsensusStepNumber: 2}
					}
					if s.sentUnwitnessMessages[stepNow-1].ConsensusStepNumber == 2 {
						// it's time to take the decision
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
						//fmt.Printf("Celebrity nodes %s \n", CelibrityNodes)

						//fmt.Printf("Celebrity Nodes  are %s \n", CelibrityNodes)

						//fmt.Printf("The set of celebrity nodes according to %s is %s \n", s.ServerIdentity(), CelibrityNodes)
						globalMaxRandomNumber := findGlobalMaxRandomNumber(s.recievedThresholdwitnessedMessages[stepNow-3])

						//globalMaxNumberOfNodes := findGlobalMaxNumberOfNodes(s.recievedThresholdwitnessedMessages[0])

						fmt.Printf("Global max random number according to %s is %d \n", s.ServerIdentity(), globalMaxRandomNumber)
						for i := 0; i < len(CelibrityNodes); i++ {

							//fmt.Printf("Json Node id is %s \n", string(CelibrityNodes[i]))

							for j := 0; j < len(s.recievedThresholdwitnessedMessages[stepNow-3]); j++ {

								jsnTwmId, _ := json.Marshal(s.recievedThresholdwitnessedMessages[stepNow-3][j].Id)
								//fmt.Printf("Json Node id from message  is %s \n", string(jsnTwmId))

								if string(CelibrityNodes[i]) == string(jsnTwmId) {

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

									if string(CelibrityNodes1[i]) == string(jsnTwmId) {

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
							// set the roster
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

								nodes := make([]*network.ServerIdentity, 0)

								for _, node := range s.roster.List {
									nodes = append(nodes, node)

								}

								strNodes = convertNetworkIdtoStringArray(nodes)
							}
							randomNumber := rand.Intn(10000)

							fmt.Printf("%s's new proposal random number is %d \n", s.ServerIdentity(), randomNumber)

							unwitnessedMessage = &template.UnwitnessedMessage{Step: stepNow, Id: s.ServerIdentity(), SentArray: convertInt2DtoString1D(s.sent, s.maxNodeCount, s.maxNodeCount), NodesProposal: strNodes, RandomNumber: randomNumber, ConsensusRoundNumber: s.sentUnwitnessMessages[stepNow-1].ConsensusRoundNumber - 1, IsConsensus: true, ConsensusStepNumber: 0}

						} else {
							// end of consensus rounds
							if s.tempConsensusNodes != nil {
								fmt.Printf("%s Updated the roster with new set of nodes\n", s.ServerIdentity())

								s.roster.List = s.tempConsensusNodes

								s.majority = len(s.roster.List)/2 + 1

								s.name = string(s.ServerIdentity().Address)


								for i := 0; i < len(s.roster.List); i++ {
									isNewNode:= true
									for j:=0; j< len(s.memberNames); j++{
										if s.memberNames[j]==string(s.roster.List[i].Address){
											isNewNode = false
											break
										}
									}
									if isNewNode{
										for j:=0; j< len(s.memberNames); j++{
											if s.memberNames[j]==""{
												s.memberNames[j] = string(s.roster.List[i].Address)
												break
											}
										}
									}

								}

								s.tempConsensusNodes = nil

							}
							unwitnessedMessage = &template.UnwitnessedMessage{Step: stepNow, Id: s.ServerIdentity(), SentArray: convertInt2DtoString1D(s.sent, s.maxNodeCount, s.maxNodeCount), IsConsensus: false}
						}

					}

				} else {
					unwitnessedMessage = &template.UnwitnessedMessage{Step: stepNow, Id: s.ServerIdentity(), SentArray: convertInt2DtoString1D(s.sent, s.maxNodeCount, s.maxNodeCount), IsConsensus: false}
				}

				if s.roster == nil {
					//fmt.Printf("%s's roster is nil \n", s.ServerIdentity())
					return
				}

				if stepNow > s.maxTime {
					return
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
				unAckedUnwitnessedMessages := s.recievedTempUnwitnessedMessages[stepNow]
				for _, uauwm := range unAckedUnwitnessedMessages {
					s.recievedUnwitnessedMessages[uauwm.Step] = append(s.recievedUnwitnessedMessages[uauwm.Step], uauwm)
					newAck := &template.AcknowledgementMessage{Id: s.ServerIdentity(), UnwitnessedMessage: uauwm, SentArray: convertInt2DtoString1D(s.sent, s.maxNodeCount, s.maxNodeCount)}
					requesterIdentity := uauwm.Id
					unicastAcknowledgementMessage(requesterIdentity, s, newAck)
					s.sentAcknowledgementMessages[uauwm.Step] = append(s.sentAcknowledgementMessages[uauwm.Step], newAck)
					//fmt.Printf("%s at %d sent ack to %s with step %d \n", s.ServerIdentity(), s.step, requesterIdentity, newAck.UnwitnessedMessage.Step)
				}

			}

		}

	}
}

//func handleCatchUpMessage(s *Service, req *template.CatchUpMessage) {
//
//	reqSentArray := convertString1DtoInt2D(req.SentArray, s)
//
//	for i := 0; i < len(s.roster.List); i++ {
//		for j := 0; j < len(s.roster.List); j++ {
//			s.sent[i][j] = max(s.sent[i][j], reqSentArray[i][j])
//		}
//	}
//
//	reqIndex := -1
//
//	for i := 0; i < len(s.roster.List); i++ {
//
//		rosterIdAtI, _ := json.Marshal(s.roster.List[i])
//
//		reqId, _ := json.Marshal(req.Id)
//
//		if string(rosterIdAtI) == string(reqId) {
//			reqIndex = i
//			break
//		}
//
//	}
//
//	s.deliv[reqIndex] = s.deliv[reqIndex] + 1
//
//	//fmt.Printf("%s at %d received catchup from %s with step %d \n", s.ServerIdentity(), s.step, req.Id, req.Step)
//
//	stepNow := s.step
//
//	if stepNow < req.Step {
//
//		catchUpMap := req.RecievedThresholdwitnessedMessages
//		for i := stepNow; i < req.Step; i++ {
//			s.recievedThresholdwitnessedMessages[i] = catchUpMap[i].Messages
//			s.recievedWitnessedMessagesBool[i] = true
//			s.step = i + 1
//			stepNow = s.step
//
//			// check if there are unwitnessed messages to which this process didn't send and ack previously
//			unAckedUnwitnessedMessages := s.recievedTempUnwitnessedMessages[stepNow]
//			for _, uauwm := range unAckedUnwitnessedMessages {
//				s.recievedUnwitnessedMessages[uauwm.Step] = append(s.recievedUnwitnessedMessages[uauwm.Step], uauwm)
//				newAck := &template.AcknowledgementMessage{Id: s.ServerIdentity(), UnwitnessedMessage: uauwm, SentArray: convertInt2DtoString1D(s.sent, s)}
//				requesterIdentity := uauwm.Id
//				unicastAcknowledgementMessage(requesterIdentity, s, newAck)
//				s.sentAcknowledgementMessages[uauwm.Step] = append(s.sentAcknowledgementMessages[uauwm.Step], newAck)
//				//fmt.Printf("%s at %d sent ack to %s with step %d \n", s.ServerIdentity(), s.step, requesterIdentity, newAck.UnwitnessedMessage.Step)
//			}
//		}
//
//		s.step = req.Step
//		stepNow = s.step
//
//		fmt.Printf("%s' increased time step to %d  with catched up \n", s.ServerIdentity(), stepNow)
//
//		unwitnessedMessage := &template.UnwitnessedMessage{Step: stepNow, Id: s.ServerIdentity(), SentArray: convertInt2DtoString1D(s.sent, s)}
//
//		if s.roster == nil {
//			//fmt.Printf("%s's roster is nil \n", s.ServerIdentity())
//			return
//		}
//
//		if stepNow > s.maxTime {
//			return
//		}
//
//		value, ok := s.sentUnwitnessMessages[stepNow]
//
//		if !ok {
//			broadcastUnwitnessedMessage(s.roster.List, s, unwitnessedMessage)
//			s.sentUnwitnessMessages[stepNow] = unwitnessedMessage
//			//fmt.Printf("%s at %d broadcast unwitnessed with step %d \n", s.ServerIdentity(), s.step, s.step)
//		} else {
//			fmt.Printf("Unwitnessed message %s for step %d from %s is already sent; possible race condition \n", value, stepNow, s.ServerIdentity())
//		}
//
//	}
//}

// newService receives the context that holds information about the node it's
// running on. Saving and loading can be done using the context. The data will
// be stored in memory for tests and simulations, and on disk for real deployments.
func newService(c *onet.Context) (onet.Service, error) {
	s := &Service{
		ServiceProcessor: onet.NewServiceProcessor(c),

		step:     0,
		stepLock: new(sync.Mutex),

		maxTime: 170,

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

		req, ok := arg1.Msg.(*template.UnwitnessedMessage)
		if !ok {
			log.Error(s.ServerIdentity(), "failed to cast to unwitnessed message")
			return nil
		}

		// check if this message can be received correctly

		myIndex := findIndexOf(s.memberNames, s.name)

		canDeleiver := true

		reqSentArray := convertString1DtoInt2D(req.SentArray, s.maxNodeCount, s.maxNodeCount)

		for i := 0; i < s.maxNodeCount; i++ {
			if s.deliv[i] < reqSentArray[i][myIndex] {
				canDeleiver = false
				break
			}
		}

		if canDeleiver {
			//fmt.Printf("INFO %s can deliver a message from %s with number %d \n", s.ServerIdentity(), req.Id, req.Number)
			handleUnwitnessedMessage(s, req)

		} else {
			// add the message to temp buffer
			//fmt.Printf("INFO %s can not deliver an unwitnessed message from %s with number %d, buffering \n", s.ServerIdentity(), req.Id, req.Step)
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

		// check if this message can be received correctly

		myIndex := findIndexOf(s.memberNames, s.name)

		canDeleiver := true

		reqSentArray := convertString1DtoInt2D(req.SentArray, s.maxNodeCount, s.maxNodeCount)

		for i := 0; i < s.maxNodeCount; i++ {
			if s.deliv[i] < reqSentArray[i][myIndex] {
				canDeleiver = false
				break
			}
		}

		if canDeleiver {
			//fmt.Printf("INFO %s can deliver a message from %s with number %d \n", s.ServerIdentity(), req.Id, req.Number)
			handleAckMessage(s, req)

		} else {
			// add the message to temp buffer
			//fmt.Printf("INFO %s can not deliver an ack message from %s with number %d, buffering \n", s.ServerIdentity(), req.Id, req.UnwitnessedMessage.Step)
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

		// check if this message can be received correctly

		myIndex := findIndexOf(s.memberNames, s.name)

		canDeleiver := true

		reqSentArray := convertString1DtoInt2D(req.SentArray, s.maxNodeCount, s.maxNodeCount)

		for i := 0; i < s.maxNodeCount; i++ {
			if s.deliv[i] < reqSentArray[i][myIndex] {
				canDeleiver = false
				break
			}
		}

		if canDeleiver {
			//fmt.Printf("INFO %s can deliver a message from %s with number %d \n", s.ServerIdentity(), req.Id, req.Number)
			handleWitnessedMessage(s, req)

		} else {
			// add the message to temp buffer
			//fmt.Printf("INFO %s can not deliver an witnessed message from %s with number %d, buffering \n", s.ServerIdentity(), req.Id, req.Step)
			s.bufferedWitnessedMessages = append(s.bufferedWitnessedMessages, req)
		}

		handleBufferedMessages(s)
		return nil
	})
	//s.RegisterProcessorFunc(catchUpMessageID, func(arg1 *network.Envelope) error {
	//	defer s.stepLock.Unlock()
	//	s.stepLock.Lock()
	//
	//	req, ok := arg1.Msg.(*template.CatchUpMessage)
	//	if !ok {
	//		log.Error(s.ServerIdentity(), "failed to cast to catch up message")
	//		return nil
	//	}
	//
	//	// check if this message can be received correctly
	//
	//	myIndex := -1
	//	for i := 0; i < len(s.roster.List); i++ {
	//
	//		rosterIdAtI, _ := json.Marshal(s.roster.List[i])
	//
	//		myServerId, _ := json.Marshal(s.ServerIdentity())
	//
	//		if string(rosterIdAtI) == string(myServerId) {
	//			myIndex = i
	//			break
	//		}
	//
	//	}
	//
	//	canDeleiver := true
	//
	//	reqSentArray := convertString1DtoInt2D(req.SentArray, s)
	//
	//	for i := 0; i < len(s.roster.List); i++ {
	//		if s.deliv[i] < reqSentArray[i][myIndex] {
	//			canDeleiver = false
	//			break
	//		}
	//	}
	//
	//	if canDeleiver {
	//		//fmt.Printf("INFO %s can deliver a message from %s with number %d \n", s.ServerIdentity(), req.Id, req.Number)
	//		handleCatchUpMessage(s, req)
	//
	//	} else {
	//		// add the message to temp buffer
	//		//fmt.Printf("INFO %s can not deliver a catchup message from %s with number %d, buffering \n", s.ServerIdentity(), req.Id, req.Step)
	//		s.bufferedCatchupMessages = append(s.bufferedCatchupMessages, req)
	//	}
	//
	//	handleBufferedMessages(s)
	//	return nil
	//})

	return s, nil
}

func handleBufferedMessages(s *Service) {

	if len(s.bufferedUnwitnessedMessages) > 0 {

		myIndex := findIndexOf(s.memberNames, s.name)

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
				//remove message from buffered requests
				//firstHalf := s.bufferedMessages[:k]
				//secondHalf := s.bufferedMessages[k+1:]
				//s.bufferedMessages = append(firstHalf, secondHalf...)
				processedBufferedMessages = append(processedBufferedMessages, k)
				//fmt.Printf("INFO %s process a buffered message from %s with number %d \n", s.ServerIdentity(), bufferedRequest.Id, bufferedRequest.Step)
				handleUnwitnessedMessage(s, bufferedRequest)
			}
		}
		for q := 0; q < len(processedBufferedMessages); q++ {
			s.bufferedUnwitnessedMessages[processedBufferedMessages[q]] = nil
		}

	}

	if len(s.bufferedAckMessages) > 0 {

		myIndex := findIndexOf(s.memberNames, s.name)

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
				//remove message from buffered requests
				//firstHalf := s.bufferedMessages[:k]
				//secondHalf := s.bufferedMessages[k+1:]
				//s.bufferedMessages = append(firstHalf, secondHalf...)
				processedBufferedMessages = append(processedBufferedMessages, k)
				//fmt.Printf("INFO %s process a buffered message from %s with number %d \n", s.ServerIdentity(), bufferedRequest.Id, bufferedRequest.UnwitnessedMessage.Step)
				handleAckMessage(s, bufferedRequest)
			}
		}
		for q := 0; q < len(processedBufferedMessages); q++ {
			s.bufferedAckMessages[processedBufferedMessages[q]] = nil
		}

	}

	if len(s.bufferedWitnessedMessages) > 0 {

		myIndex := findIndexOf(s.memberNames, s.name)

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
				//remove message from buffered requests
				//firstHalf := s.bufferedMessages[:k]
				//secondHalf := s.bufferedMessages[k+1:]
				//s.bufferedMessages = append(firstHalf, secondHalf...)
				processedBufferedMessages = append(processedBufferedMessages, k)
				//fmt.Printf("INFO %s process a buffered message from %s with number %d \n", s.ServerIdentity(), bufferedRequest.Id, bufferedRequest.Step)
				handleWitnessedMessage(s, bufferedRequest)
			}
		}
		for q := 0; q < len(processedBufferedMessages); q++ {
			s.bufferedWitnessedMessages[processedBufferedMessages[q]] = nil
		}

	}
	//if len(s.bufferedCatchupMessages) > 0 {
	//
	//	myIndex := -1
	//	for i := 0; i < len(s.roster.List); i++ {
	//
	//		rosterIdAtI, _ := json.Marshal(s.roster.List[i])
	//
	//		myServerId, _ := json.Marshal(s.ServerIdentity())
	//
	//		if string(rosterIdAtI) == string(myServerId) {
	//			myIndex = i
	//			break
	//		}
	//
	//	}
	//
	//	processedBufferedMessages := make([]int, 0)
	//	for k := 0; k < len(s.bufferedCatchupMessages); k++ {
	//
	//		bufferedRequest := s.bufferedCatchupMessages[k]
	//
	//		if bufferedRequest == nil {
	//			continue
	//		}
	//
	//		canDeleiver := true
	//		reqSentArray := convertString1DtoInt2D(bufferedRequest.SentArray, s)
	//
	//		for i := 0; i < len(s.roster.List); i++ {
	//			if s.deliv[i] < reqSentArray[i][myIndex] {
	//				canDeleiver = false
	//				break
	//			}
	//		}
	//
	//		if canDeleiver {
	//			//remove message from buffered requests
	//			//firstHalf := s.bufferedMessages[:k]
	//			//secondHalf := s.bufferedMessages[k+1:]
	//			//s.bufferedMessages = append(firstHalf, secondHalf...)
	//			processedBufferedMessages = append(processedBufferedMessages, k)
	//			//fmt.Printf("INFO %s process a buffered message from %s with number %d \n", s.ServerIdentity(), bufferedRequest.Id, bufferedRequest.Step)
	//			handleCatchUpMessage(s, bufferedRequest)
	//		}
	//	}
	//	for q := 0; q < len(processedBufferedMessages); q++ {
	//		s.bufferedCatchupMessages[processedBufferedMessages[q]] = nil
	//	}
	//
	//}

}
