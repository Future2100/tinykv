// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"
	"math/rand"

	"github.com/cznic/mathutil"

	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	// id is unique identifier within the raft group(maybe it is global unique)
	id uint64

	// Term starts with 0, and is incremented by 1 every time
	Term uint64
	// Vote indicates which candidate current node votes for. None means no vote.
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}

	storage := c.Storage
	hs, _, err := storage.InitialState()
	if err != nil {
		panic(err.Error())
	}
	prs := make(map[uint64]*Progress)
	if len(c.peers) != 0 {
		for _, id := range c.peers {
			prs[id] = &Progress{Next: 1, Match: 0}
		}
	}

	return &Raft{
		id: c.ID,

		Term: hs.Term,
		Vote: hs.Vote,

		RaftLog: newLog(c.Storage),

		Prs: prs,

		State: StateFollower,

		votes: make(map[uint64]bool),

		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
	}
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	progress := r.Prs[to]
	if progress == nil {
		return false
	}

	beginIdx := progress.Next

	entries := r.RaftLog.entriesFrom(beginIdx)
	if len(entries) == 0 {
		return false
	}

	entryPtrs := make([]*pb.Entry, len(entries))
	for i, entry := range entries {
		e := entry
		entryPtrs[i] = &e
	}

	logTerm, err := r.RaftLog.Term(beginIdx - 1)
	if err != nil {
		return false
	}

	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Entries: entryPtrs,
		Index:   beginIdx - 1,
		LogTerm: logTerm,
		Commit:  r.RaftLog.committed,
	})

	progress.Next += uint64(len(entryPtrs))

	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	r.msgs = append(r.msgs, pb.Message{
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Commit:  r.RaftLog.committed,
		MsgType: pb.MessageType_MsgHeartbeat,
	})
}

func (r *Raft) sendRequestVoteToPeers() {
	if r.Lead != None {
		return
	}

	logIndex := r.RaftLog.LastIndex()
	logTerm, _ := r.RaftLog.Term(logIndex)
	for id, _ := range r.Prs {
		if id == r.id {
			continue
		}
		r.msgs = append(r.msgs, pb.Message{
			From:    r.id,
			To:      id,
			Term:    r.Term,
			LogTerm: logTerm,
			Index:   logIndex,
			MsgType: pb.MessageType_MsgRequestVote,
		})
	}
}

func (r *Raft) majority() int {
	return len(r.Prs)/2 + 1
}

func (r *Raft) votedMajority() bool {
	count := 0
	for _, vote := range r.votes {
		if vote {
			count++
		}
	}

	return count >= r.majority()
}

func (r *Raft) rejectedMajority() bool {
	count := 0
	for _, vote := range r.votes {
		if !vote {
			count++
		}
	}

	return count >= r.majority()
}

func (r *Raft) isLeader() bool {
	return r.State == StateLeader
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	r.electionElapsed++

	if r.isLeader() {
		r.heartbeatElapsed++
	}

	if r.heartbeatElapsed >= r.heartbeatTimeout {
		if err := r.Step(pb.Message{MsgType: pb.MessageType_MsgBeat}); err != nil {
			panic(err.Error())
		}
	}

	if r.electionElapsed >= r.electionTimeout {
		if !r.isLeader() {
			if err := r.Step(pb.Message{From: r.id, To: r.id, MsgType: pb.MessageType_MsgHup}); err != nil {
				panic(err.Error())
			}
		}
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.Term = term
	r.Lead = lead
	r.State = StateFollower
	r.heartbeatElapsed = 0
	r.resetElectionElapsed()
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.Term++
	r.Lead = None
	r.Vote = r.id
	r.State = StateCandidate
	r.votes = make(map[uint64]bool)

	r.resetElectionElapsed()

	r.votes[r.id] = true

	if r.votedMajority() {
		r.becomeLeader()
	}
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.State = StateLeader
	r.Lead = r.id
	r.heartbeatElapsed = 0

	for _, pr := range r.Prs {
		pr.Match = r.RaftLog.LastIndex()
		pr.Next = r.RaftLog.LastIndex() + 1
	}

	err := r.Step(pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		Entries: []*pb.Entry{
			{},
		},
	})

	if err != nil {
		panic(err.Error())
	}
}

func (r *Raft) sendUnsentEntriesToPeers() {
	for id, _ := range r.Prs {
		if id == r.id {
			continue
		}

		r.sendAppend(id)
	}
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower:
		switch m.MsgType {
		case pb.MessageType_MsgAppend:
			r.handleAppendEntries(m)
		case pb.MessageType_MsgHup:
			r.becomeCandidate()
			r.sendRequestVoteToPeers()
		case pb.MessageType_MsgRequestVote:
			higherTerm := m.Term > r.Term

			if m.Term < r.Term {
				r.appendMsg(pb.Message{
					From:    r.id,
					To:      m.From,
					Term:    r.Term,
					Reject:  true,
					MsgType: pb.MessageType_MsgRequestVoteResponse,
				})

				return nil
			} else {
				r.Term = m.Term
			}

			rejectFromLog := r.rejectByUpdateToDate(m)

			if rejectFromLog {
				r.appendMsg(pb.Message{
					From:    r.id,
					To:      m.From,
					Term:    r.Term,
					Reject:  true,
					MsgType: pb.MessageType_MsgRequestVoteResponse,
				})

				return nil
			}

			// term > current term or vote for None
			if higherTerm || r.Vote == None {
				r.Vote = m.From
				r.Term = m.Term
			}

			r.appendMsg(pb.Message{
				From:    r.id,
				To:      m.From,
				Term:    r.Term,
				Reject:  !(r.Vote == m.From),
				MsgType: pb.MessageType_MsgRequestVoteResponse,
			})
		case pb.MessageType_MsgHeartbeat:
			r.handleHeartbeat(m)
		}
	case StateCandidate:
		switch m.MsgType {
		case pb.MessageType_MsgAppend:
			if r.Term <= m.Term {
				r.Vote = None
				r.becomeFollower(m.Term, m.From)
			}
			r.RaftLog.appendEntries(m.Term, m.Entries)
			r.appendMsg(pb.Message{
				From:    r.id,
				To:      m.From,
				Term:    r.Term,
				MsgType: pb.MessageType_MsgAppendResponse,
			})
		case pb.MessageType_MsgHup:
			r.becomeCandidate()
			r.sendRequestVoteToPeers()
		case pb.MessageType_MsgRequestVoteResponse:
			r.votes[m.From] = !m.Reject

			if r.votedMajority() {
				r.becomeLeader()
			}

			if r.rejectedMajority() {
				r.becomeFollower(r.Term, None)
			}

		case pb.MessageType_MsgRequestVote:
			vote := r.Term < m.Term
			if vote {
				r.Vote = m.From
				r.becomeFollower(m.Term, None)
			}

			r.appendMsg(pb.Message{
				From:    r.id,
				To:      m.From,
				Term:    r.Term,
				Reject:  !vote,
				MsgType: pb.MessageType_MsgRequestVoteResponse,
			})
		case pb.MessageType_MsgHeartbeat:
			r.handleHeartbeat(m)
		}
	case StateLeader:
		switch m.MsgType {
		case pb.MessageType_MsgAppend:
			if r.Term < m.Term {
				r.becomeFollower(m.Term, m.From)
			}
			r.appendMsg(pb.Message{
				From:    r.id,
				To:      m.From,
				Term:    r.Term,
				MsgType: pb.MessageType_MsgAppendResponse,
			})
		case pb.MessageType_MsgPropose:
			r.RaftLog.appendEntries(r.Term, m.Entries)
			prs := r.Prs[r.id]
			prs.Match = r.RaftLog.LastIndex()
			prs.Next = prs.Match + 1

			if r.majority() == 1 {
				r.RaftLog.commitTo(r.RaftLog.LastIndex())
			}
			r.sendUnsentEntriesToPeers()
		case pb.MessageType_MsgAppendResponse:
			if m.Reject {
				r.Prs[m.From].Next = m.Index
				r.sendAppend(m.From)
				break
			}

			term, err := r.RaftLog.Term(m.Index)
			if err != nil {
				return err
			}
			if term != r.Term {
				break
			}

			// commit append response will be ignored
			if r.Prs[m.From].Match == m.Index {
				break
			}

			r.Prs[m.From].Match = m.Index
			r.Prs[m.From].Next = m.Index // will plus 1 when the entry is committed

			count := 0
			maj := r.majority()
			for _, pr := range r.Prs {
				if pr.Match >= m.Index {
					count++
				}

				if count >= maj {
					break
				}
			}

			if count < maj {
				return nil
			}

			r.RaftLog.commitTo(m.Index)

			for id, pr := range r.Prs {
				if r.id == id {
					continue
				}
				if pr.Match == pr.Next && pr.Match == m.Index {
					pr.Next = pr.Match + 1
					// commit log
					r.appendMsg(pb.Message{
						From:    r.id,
						To:      id,
						Term:    r.Term,
						Index:   m.Index,
						LogTerm: m.LogTerm,
						Commit:  r.RaftLog.committed,
						MsgType: pb.MessageType_MsgAppend,
					})
				}
			}
		case pb.MessageType_MsgBeat:
			r.heartbeatElapsed = 0
			for id, _ := range r.Prs {
				if id != r.id {
					r.sendHeartbeat(id)
				}
			}
		case pb.MessageType_MsgHeartbeatResponse:
			r.Prs[m.From].Match = m.Index
			r.Prs[m.From].Next = m.Index + 1
			if m.Index < r.RaftLog.LastIndex() {
				r.sendAppend(m.From)
			}
		case pb.MessageType_MsgRequestVote:
			reject := m.Term <= r.Term

			if m.Term > r.Term {
				r.becomeFollower(m.Term, None)
			}

			rejectFromLog := r.rejectByUpdateToDate(m)

			reject = reject || rejectFromLog
			if !reject {
				r.Vote = m.From
			}

			r.appendMsg(pb.Message{
				From:    r.id,
				To:      m.From,
				Term:    r.Term,
				Reject:  reject,
				MsgType: pb.MessageType_MsgRequestVoteResponse,
			})
		}
	}
	return nil
}

func (r *Raft) rejectByUpdateToDate(m pb.Message) bool {
	// the same term
	lastIndex := r.RaftLog.LastIndex()
	lastEntryTerm, _ := r.RaftLog.Term(lastIndex)

	rejectFromLog := true
	if m.LogTerm != lastEntryTerm {
		rejectFromLog = m.LogTerm < lastEntryTerm
	} else {
		rejectFromLog = m.Index < lastIndex
	}

	return rejectFromLog
}

// resetElectionElapsed will assign a random negative value to electionElapsed to simulate random election timeout
func (r *Raft) resetElectionElapsed() {
	r.electionElapsed = -rand.Int() % r.electionTimeout
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	if r.Term > m.Term {
		r.appendMsg(pb.Message{
			From:    r.id,
			To:      m.From,
			Reject:  true,
			Term:    r.Term,
			MsgType: pb.MessageType_MsgAppendResponse,
		})

		return
	}

	r.Term = m.Term
	r.Lead = m.From
	r.Vote = None

	term, err := r.RaftLog.Term(m.Index)
	if err != nil || m.LogTerm != term {

		r.appendMsg(pb.Message{
			From:    r.id,
			To:      m.From,
			Reject:  true,
			Term:    r.Term,
			Index:   m.Index,
			MsgType: pb.MessageType_MsgAppendResponse,
		})
		return
	}

	r.RaftLog.appendEntriesFromIndex(m.Index, m.Entries)

	newlyAppendIdx := m.Index + uint64(len(m.Entries))
	lastTerm, err := r.RaftLog.Term(newlyAppendIdx)
	if err != nil {
		log.Panic(err.Error())
	}

	if m.Commit > r.RaftLog.committed {
		commitIdx := mathutil.MinUint64(newlyAppendIdx, m.Commit)
		r.RaftLog.commitTo(commitIdx)
	}

	r.appendMsg(pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		From:    r.id,
		To:      m.From,
		Reject:  false,
		Term:    r.Term,
		LogTerm: lastTerm,
		Index:   newlyAppendIdx,
	})
}

func (r *Raft) appendMsg(m pb.Message) {
	r.msgs = append(r.msgs, m)
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	r.Vote = None
	r.becomeFollower(m.Term, m.From)
	if m.Commit > r.RaftLog.committed {
		committedIndex := mathutil.MinUint64(m.Commit, r.RaftLog.LastIndex())
		r.RaftLog.commitTo(committedIndex)
	}
	lastIndex := r.RaftLog.LastIndex()
	lastTerm, err := r.RaftLog.Term(lastIndex)
	if err != nil {
		log.Panic(err)
	}

	r.appendMsg(pb.Message{
		From:    r.id,
		To:      m.From,
		Term:    r.Term,
		LogTerm: lastTerm,
		Index:   lastIndex,
		MsgType: pb.MessageType_MsgHeartbeatResponse,
	})
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
