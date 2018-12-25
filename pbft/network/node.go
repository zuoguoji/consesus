package network

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

type Node struct {
	NodeID        string
	NodeTable     map[string]string
	View          *View
	CurrentState  *consensus.CurrentState
	CommittedMsgs []*MsgBuffer
	MsgBuffer     *MsgBuffer
	MsgEntrance   chan interface{}
	MsgDelivery   chan interface{}
	Alarm         chan bool
}

type MsgBuffer struct {
	ReqMsgs        []*consensus.RequestMsg
	PrePrepareMsgs []*consensus.PrePrepareMsg
	PrepareMsgs    []*consensus.VoteMsg
	CommitMsgs     []*consensus.VoteMsg
}

type View struct {
	ID      int64
	Primary string
}

const ResolvingTimeDuration = time.Millisecond * 1000

func NewNode(nodeID string) *Node {
	const viewID = 10000000000

	node := &Node{
		NodeID: nodeID,
		NodeTable: map[string]string{
			"Apple":  "localhost:1111",
			"MS":     "localhost:1112",
			"Google": "localhost:1113",
			"IBM":    "localhost:1114",
		},
		View: &View{
			ID:      viewID,
			Primary: "Apple",
		},
		CurrentState:  nil,
		CommittedMsgs: make([]*consensus.RequestMsg, 0),
		MsgBuffer: &MsgBuffer{
			ReqMsgs:       make([]*consensus.RequestMsg, 0),
			PrePrepareMsg: make([]*consensus.PrePrepareMsg, 0),
			PrepareMsgs:   make([]*consensus.VoteMsg, 0),
			CommitMsgs:    make([]*consensus.VoteMsg, 0),
		},
		MsgEntrance: make(chan interface{}),
		MsgDelivery: make(chan interface{}),
		Alarm:       make(chan bool),
	}

	go node.dispatchMsg()

	go node.alarmToDispatcher()

	go node.resolveMsg()

	return node
}

func (node *NOde) Broadcast(msg interface{}, path string) map[string]error {
	errorMap := make(map[string]error)

	for nodeID, url := range node.NodeTable {
		if nodeID == node.NodeID {
			continue
		}

		jsonMsg, err := json.Marshal(msg)
		if err != nil {
			errorMap[nodeID] = err
			continue
		}

		send(url+path, jsonMsg)
	}
	if len(errorMap) == 0 {
		return nil
	} else {
		return errorMap
	}
}

func (node *Node) Reply(msg *consensus.ReplyMsg) error {
	for _, value := range node.CommittedMsgs {
		fmt.Printf("Commited value: %s, %d, %s, %d", value.ClientID, value.Timestamp, value.Operation, value.SequenceID)
	}
	fmt.Print("\n")

	jsonMsg, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	send(node.NodeTable[node.View.Primary]+"/reply", jsonMsg)

	return nil
}

func (node *Node) GetReq(reqMsg *consensus.RequestMsg) error {
	LogMsg(reqMsg)

	err := node.createStateForNewConsensus()
	if err != nil {
		return err
	}

	prePrepareMsg, err := node.CurrentState.StartConsensus(reqMsg)
	if err != nil {
		return err
	}

	LogStage(fmt.Sprint("Consensus Process (ViewID: %d)", node.CurrentState.ViewID), false)

	if prePrepareMsg != nil {
		node.Broadcast(prePrepareMsg, "/preprepare")
		LogStage("Pre-prepare", true)
	}

	return nil
}

func (node *Node) GetPrepare(prePrepareMsg *consensus.PrePrepareMsg) error {
	LogMsg(prePrapareMsg)

	err := node.createStateForNewConsensus()
	if err != nil {
		return err
	}

	prePareMsg, err := node.currentState.PrePrepare(prePrepareMsg)
	if err != nil {
		return err
	}

	if prePareMsg != nil {
		prePareMsg.NodeID = node.NodeID
		LogStage("Pre-prepare", true)
		node.Broadcast(prePareMsg, "/prepare")
		LogStage("Prepare", false)
	}

	return nil
}

func (node *Node) GetCommit(commitMsg *consensus.VoteMsg) error {
	LogMsg(commitMsg)

	replyMsg, committedMsg, err := node.CurrentState.Commit(commitMsg)
	if err != nil {
		return err
	}

	if replyMsg != nil {
		if committedMsg == nil {
			return errors.New("committed message is nil, even though the reply message is not nil")
		}

		replyMsg.NodeID = node.NodeID

		node.CommittedMsgs = append(node.CommittedMsgs, committedMsg)

		LogStage("Commit", true)
		node.Reply(replyMsg)
		LogStage("Reply", true)
	}

	return nil
}

// 获取响应
func (node *Node) GetReply(msg *consensus.ReplyMsg) {
	fmt.Printf("Result: %s by %s\n", msg.Result, msg.NodeID)
}

// 新建新共识
func (node *Node) createStateForNewConsensus() error {
	// 判断是否还有共识在进行中
	if node.CurrentState != nil {
		return errors.New("another consensus is ongoing")
	}
	// 获取最后sequenceID
	var lastSequenceID int64
	if len(node.CommittedMsgs) == 0 {
		lastSequenceID = -1
	} else {
		lastSequenceID = node.CommittedMsgs[len(node.CommittedMsgs)-1].SequenceID
	}
	// 创建的一个新的预选共识进程状态
	node.CurrentState = consensus.CreateState(node.View.ID, lastSequenceID)

	logState("Create the replica status", true)

	return nil
}

func (node *Node) dispatchMsg() {
	for {
		select {
		case msg := <-node.MsgEntrance:
			err := node.routeMsg(msg)
			if err != nil {
				fmt.Println(err)
			}
		case <-node.Alarm:
			err := node.routeMsgWhenAlarmed()
			if err != nil {
				fmt.Println(err)
			}
		}
	}
}

func (node *Node) routeMsg(msg interface{}) []error {
	switch msg.(type) {
	case *consensus.RequestMsg:
		if node.CurrentState == nil {
			msgs := make([]*consensus.RequestMsg, len(node.MsgBuffer.ReqMsgs))
			copy(msgs, nose.MsgBuffer.ReqMsgs)

			msgs = append(msgs, msg.(*consensus.RequestMsg))

			node.MsgBuffer.ReqMsgs = make([]*consensus.RequestMsg, 0)

			node.MsgDelivery <- msgs
		} else {
			node.MsgBuffer.ReqMsgs = append(node.MsgBuffer.ReqMsgs, msg.(*consensus.RequestMsg))
		}
	case *consensus.PrePrepareMsg:
		if node.CurrentState == nil {
			msgs := make([]*consensus.PrePrepareMsg, len(node.MsgBuffer.PrePrepareMsgs))
			copy(msgs, node.MsgBuffer.PrePrepareMsgs)

			msgs = append(msgs, msg.(*consensus.PrePrepareMsg))

			node.MsgBuffer.PrePreparemsgs = make([]*consensus.PrePrepareMsg, 0)

			node.MsgDelivery <- msgs
		} else {
			node.MsgBuffer.PrePrepareMsgs = append(node.MsgBuffer.PrePrepareMsgs, msg.(*consensus.prePrepareMsg))
		}
	case *consensus.VoteMsg:
		if msg.(*consensus.VoteMsg).MsgType == consensus.PrepareMsg {
			if node.CurrentState == nil || node.CurrentState.CurrentState != consensus.PrePrepared {
				nodenode.MsgBuffer.PrepareMsgs = append(node.MsgBuffer.PrepareMsgs, msg.(*consensus.VoteMsg))
			} else {
				msgs := make([]*consensus.VoteMsg, len(node.MsgBuffer.PrepareMsgs))
				copy(msgs, node.MsgBuffer.PrepareMsgs)

				msgs = append(msgs, msg.(*consensus.VoteMsg))

				node.MsgBuffer.PrepareMsgs = make([]*consensus.VoteMsg, 0)

				node.MsgDelivery <- msgs
			}
		} else if msg.(*consensus.VoteMsg).MsgType == consensus.CommitMsg {
			if node.CurrentState.CurrentState == nil || node.CurrentState.CurrentState != consensus.Prepared {
				node.MsgBuffer.CommitMsgs = append(node.MsgBuffer.CommitMsgs, msg.(*consensus.VoteMsg))
			} else {
				msgs := make([]*consensus.VoteMsg, len(node.MsgBuffer.CommitMSgs))
				copy(msgs, node.MsgBuffer.CommitMsgs)

				msgs = append(msgs, msg.(*consensus.VoteMsg))

				node.MsgBuffer.CommitMsgs = make([]*consensus.VoteMsg, 0)

				node.MsgDelivery <- msgs
			}
		}
	}
	return nil
}

func (node *Node) routeMsgWhenAlarmed() []error {
	if node.CurrentState == nil {
		if len(node.MsgBuffer.ReqMsgs) != 0 {
			msgs := make([]*consensus.RequestMsg, len(node.MsgBuffer.ReqMsgs))
			copy(msgs, node.MsgBuffer.ReqMsgs)

			node.MsgDelivery <- msgs
		}

		if len(node.MsgBuffer.PrePrepareMsgs) != 0 {
			msgs := make([]*consensus.PrePrepareMsg, len(node.MsgBuffer.PrePrepareMsgs))
			copy(msgs, node.MsgBuffer.PrePrepareMsgs)

			node.MsgDelivery <- msgs
		}
	} else {
		switch node.CurrentState.CurrentState {
		case consensus.PrePrepared:
			if len(node.MsgBuffer.PrepareMsgs) != 0 {
				msgs := make([]*consensus.VoteMsg, len(node.MsgBuffer.PrepareMsgs))
				copy(msgs, node.MsgBuffer.PrepareMsgs)

				node.MsgDelivery <- msgs
			}
		case consensus.Prepared:
			if len(node.MsgBuffer.CommitMsgs) != 0 {
				msgs := make([]*consensus.VoteMsg, len(node.MsgBuffer.CommitMsgs))
				copy(msgs, node.MsgsBuffer.commitMsgs)

				node.MsgDelivery <- msgs
			}
		}
	}
	return nil
}

func (node *Node) resolveMsg() {
	for {
		msgs := <-node.MsgDelivery
		switch msgs.(type) {
		case []*consensus.RequestMsg:
			errs := node.ResolveRequestMsg(msgs.([]*consensus.RequestMsg))
			if len(errs) != 0 {
				for _, err := range errs {
					fmt.Println(err)
				}
			}
		case []*consensus.PrePrepareMsg:
			errs := node.resolvePrePrepareMsg(msgs.([]*consensus.PrePrepareMsg))
			if len(errs) != 0 {
				for _, err := range errs {
					fmt.Println(err)
				}
			}
		case []*consensus.VoteMsg:
			voteMsgs := msgs.([]*consensus.VoteMsg)
			if len(errs) != 0 {
				break
			}

			if voteMsgs[0].MsgType == consensus.PrepareMsg {
				errs := node.resolvePrepareMsg(voteMsgs)
				if len(errs) != 0 {
					for _, err := range errs {
						fmt.Println(err)
					}
				}
			} else if voteMsgs[0].MsgType == consensus.CommitMsg {
				errs := node.resolveCommitMsg(voteMsgs)
				if len(errs) != 0 {
					for _, err := range errs {
						fmt.Println(err)
					}
				}
			}
		}
	}
}

func (node *Node) alarmToDispatcher() {
	for {
		time.Sleep(ResolvingTimeDuration)
		node.Alarm <- true
	}
}

func (node *Node) resolveRequestMsg(msgs []*consensus.RequestMsg) []error {
	errs := make([]error, 0)

	for _, reqMsg := range msgs {
		err := node.GetReq(reqMsg)
		if err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) != 0 {
		return errs
	}

	return nil
}

func (node *Node) resolvePrePrepareMsg(msgs []*consensus.PrePrepareMsg) []error {
	errs := make([]error, 0)

	for _, prePrepareMsg := range msgs {
		err := node.GetPrePrepare(prePrepareMsg)
		if err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) != 0 {
		return errs
	}

	return nil
}

func (node *Node) resolvePrepareMsg(msgs []*consensus.VoteMsg) []error {
	errs := make([]error, 0)

	for _, prepareMsg := range msgs {
		err := node.GetPrepare(prepareMsg)
		if err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) != 0 {
		return errs
	}

	return nil
}

func (node *Node) resolveCommitMsg(msgs []*consensus.VoteMsg) []error {
	errs := make([]error, 0)

	for _, commitMsg := range msgs {
		err := node.GetCommit(commitMsg)
		if err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) != 0 {
		return errs
	}

	return nil
}
