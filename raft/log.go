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
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// RaftLog manage the log entries, its struct look like:
//
//  snapshot/first.....applied....committed....stabled.....last
//  --------|------------------------------------------------|
//                            log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	hs, _, err := storage.InitialState()
	if err != nil {
		panic(err.Error())
	}

	firstIdx, err := storage.FirstIndex()
	if err != nil {
		panic(err.Error())
	}

	lastIdx, err := storage.LastIndex()
	if err != nil {
		panic(err.Error())
	}

	entries, err := storage.Entries(firstIdx, lastIdx+1)
	if err != nil {
		panic(err.Error())
	}

	return &RaftLog{
		storage:         storage,
		committed:       hs.Commit,
		applied:         0,
		stabled:         lastIdx,
		entries:         entries,
		pendingSnapshot: nil,
	}
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	if len(l.entries) == 0 {
		return nil
	}

	firstIdx := l.entries[0].Index
	if l.stabled < firstIdx {
		return l.entries
	}

	return l.entries[l.stabled-firstIdx+1:]
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	// (applied, committed]
	if len(l.entries) == 0 {
		return nil
	}

	firstIdx := l.entries[0].Index
	return l.entries[l.applied-firstIdx+1 : l.committed-firstIdx+1]
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	if len(l.entries) == 0 {
		if lastIdx, err := l.storage.LastIndex(); err != nil {
			return 0
		} else {
			return lastIdx
		}
	}

	return l.entries[0].Index + uint64(len(l.entries)) - 1
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	if i == 0 {
		return 0, nil
	}

	if len(l.entries) == 0 {
		return 0, ErrUnavailable
	}
	firstIdx := l.entries[0].Index
	lastIdx := firstIdx + uint64(len(l.entries)) - 1

	if i > lastIdx {
		return 0, ErrUnavailable
	}
	if i >= firstIdx {
		return l.entries[i-firstIdx].Term, nil
	}

	if term, err := l.storage.Term(i); err != nil {
		return 0, err
	} else {
		return term, nil
	}
}

func (l *RaftLog) appendEntries(term uint64, es []*pb.Entry) {
	idx := l.LastIndex() + 1
	for _, entry := range es {
		entry.Index = idx
		entry.Term = term

		l.entries = append(l.entries, *entry)

		idx++
	}
}

//  index equals to es[0].Index - 1
func (l *RaftLog) appendEntriesFromIndex(index uint64, es []*pb.Entry) {
	if len(es) == 0 {
		return
	}

	if index == 0 && len(l.entries) == 0 {
		for _, e := range es {
			l.entries = append(l.entries, *e)
		}

		return
	}

	offset := l.entries[0].Index
	j := 0
	first := true

	for i := int(index-offset) + 1; i < len(l.entries) && j < len(es); i++ {
		iEntry := l.entries[i]
		jEntry := es[j]
		if first && (iEntry.Term != jEntry.Term || iEntry.Index != jEntry.Index) {
			first = false
			var tmpStable uint64

			if i != 0 {
				tmpStable = l.entries[i-1].Index
			} else {
				tmpStable = jEntry.Index - 1
			}

			if tmpStable < l.stabled {
				l.stabled = tmpStable
			}

			l.entries = l.entries[:i+1]
		}
		l.entries[i] = *es[j]

		j++
	}

	if j < len(es) {
		for ; j < len(es); j++ {
			l.entries = append(l.entries, *es[j])
		}
	}
}

func (l *RaftLog) uncommittedEntries() []pb.Entry {
	if len(l.entries) == 0 {
		return nil
	}

	if l.entries[0].Index > l.committed {
		return l.entries[:]
	}

	return l.entries[l.committed-l.entries[0].Index+1:]
}

func (l *RaftLog) commitTo(i uint64) {
	if i > l.committed {
		l.committed = i
	}
}

// entriesFrom return the entries from the given index
func (l *RaftLog) entriesFrom(from uint64) []pb.Entry {
	if len(l.entries) == 0 {
		return nil
	}

	firstIdx := l.entries[0].Index
	if from >= firstIdx {
		return l.entries[from-firstIdx:]
	}

	storageLastIdx, err := l.storage.LastIndex()
	if err != nil || storageLastIdx < from {
		return nil
	}

	entriesFromStorage, err := l.storage.Entries(from, storageLastIdx)
	if err != nil {
		return nil
	}

	// TODO: need to check the continuity of entries
	return append(entriesFromStorage, l.entries...)
}

func (l *RaftLog) entry(idx uint64) *pb.Entry {
	if len(l.entries) == 0 {
		return nil
	}
	firstIdx := l.entries[0].Index
	lastIdx := firstIdx + uint64(len(l.entries)) - 1
	if idx > lastIdx {
		return nil
	}

	if idx >= firstIdx && idx <= lastIdx {
		return &l.entries[idx-firstIdx]
	}

	storageLastIdx, err := l.storage.LastIndex()
	if err != nil || storageLastIdx < idx {
		return nil
	}

	entryFromStorage, err := l.storage.Entries(idx, idx+1)
	if err != nil {
		return nil
	}

	return &entryFromStorage[0]
}
