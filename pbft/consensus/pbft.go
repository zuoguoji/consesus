package consensus

type PBFT interface {
	StartConsensus(requestMsg *RequestMsg) (*PrePrepareMsg, error)
	PrePrepare(prePrepareMsg *PrePrepareMsg) (*VoteMsg, error)
	Prepare(prepareMsg *VoteMsg) (*VoteMsg, error)
	Commit(CommitMsg *VoteMsg) (*ReplyMsg, *RequestMsg, error)
}
