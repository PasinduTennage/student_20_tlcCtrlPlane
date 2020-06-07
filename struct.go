package template

/*
This holds the messages used to communicate with the service over the network.
*/

import (
	"go.dedis.ch/onet/v3"
	"go.dedis.ch/onet/v3/network"
)

// We need to register all messages so the network knows how to handle them.
func init() {
	network.RegisterMessages(
		Count{}, CountReply{},
		Clock{}, ClockReply{},
	)
}

const (
	// ErrorParse indicates an error while parsing the protobuf-file.
	ErrorParse = iota + 4000
)

// Clock will run the tepmlate-protocol on the roster and return
// the time spent doing so.
type Clock struct {
	Roster *onet.Roster
}

// ClockReply returns the time spent for the protocol-run.
type ClockReply struct {
	Time     float64
	Children int
}

// Count will return how many times the protocol has been run.
type Count struct {
}

// CountReply returns the number of protocol-runs
type CountReply struct {
	Count int
}

type UnwitnessedMessage struct {
	Step                 int
	Id                   *network.ServerIdentity
	SentArray            []string
	RandomNumber         int
	NodesProposal        []string
	Nodes                []string
	ConsensusRoundNumber int

	ConsensusStepNumber int
	PingDistances       []string
	PingMetrix          []string

	Messagetype int // 0: tlc, 1: ping distances, 2: nodes consensus, 3: ping consensus

	FoundConsensus           bool
	PingMulticastRoundNumber int
}

type WitnessedMessage struct {
	Step int
	Id   *network.ServerIdentity
	//acknowledgeSet list.List how to define a list
	SentArray            []string
	RandomNumber         int
	NodesProposal        []string
	Nodes                []string
	ConsensusRoundNumber int

	ConsensusStepNumber      int
	PingDistances            []string
	PingMetrix               []string
	Messagetype              int
	FoundConsensus           bool
	PingMulticastRoundNumber int
}

type AcknowledgementMessage struct {
	Id                 *network.ServerIdentity
	UnwitnessedMessage *UnwitnessedMessage
	SentArray          []string
}

type InitRequest struct {
}

// SignatureResponse is what the Cosi service will reply to clients.
type InitResponse struct {
}

type GenesisNodesRequest struct {
	Nodes []string
}

// SignatureResponse is what the Cosi service will reply to clients.
type GenesisNodesResponse struct {
}

type CatchUpMessage struct {
	Id                                 *network.ServerIdentity
	Step                               int
	RecievedThresholdwitnessedMessages map[int]*ArrayWitnessedMessages
	SentArray                          []string
}

type ArrayWitnessedMessages struct {
	Messages []*WitnessedMessage
}

type JoinRequest struct {
}

// SignatureResponse is what the Cosi service will reply to clients.
type JoinResponse struct {
}

type NodeJoinRequest struct {
	Id *network.ServerIdentity
}

// SignatureResponse is what the Cosi service will reply to clients.
type NodeJoinResponse struct {
	Id *network.ServerIdentity
}

type NodeJoinConfirmation struct {
	//ackedNodes []string
	Id *network.ServerIdentity
}

type JoinAdmissionCommittee struct {
	NewCommitee []string
	Step        int
}

type RosterNodesRequest struct {
	Roster *onet.Roster
}

// SignatureResponse is what the Cosi service will reply to clients.
type RosterNodesResponse struct {
}
