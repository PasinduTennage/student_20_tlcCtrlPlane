package service

import (
	"github.com/dedis/cothority_template/gentree"
	"go.dedis.ch/onet/v3"
	"go.dedis.ch/onet/v3/network"
)

/* Message structure used to broadcast the unwitnessed messages*/
type UnwitnessedMessage struct{
	step int
	Id   string
}

type WitnessedMessage struct{
	step int
	id string
	//acknowledgeSet list.List how to define a list
}

type AcknowledgementMessage struct{
	acknowledgement string
	id string
	unwitnessedMessage UnwitnessedMessage
}

type InitRequest struct {
	Nodes                []*gentree.LocalityNode
	ServerIdentityToName map[*network.ServerIdentity]string
	NrOps int
	OpIdxStart int
	Roster *onet.Roster
}


// SignatureResponse is what the Cosi service will reply to clients.
type InitResponse struct {
}