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

import pb "go.etcd.io/etcd/raft/raftpb"

// unstable.entries[i] has raft log position i+unstable.offset.
// Note that unstable.offset may be less than the highest log
// position in storage; this means that the next write to storage
// might need to truncate the log before persisting unstable.entries.
type unstable struct {
	// the incoming unstable snapshot, if any.
	snapshot *pb.Snapshot
	// all entries that have not yet been written to storage.
	entries []pb.Entry
	offset  uint64

	logger Logger
}

// maybeFirstIndex returns the index of the first possible entry in entries
// if it has a snapshot.
// 如果有快照，则获取entries的第一条索引
func (u *unstable) maybeFirstIndex() (uint64, bool) {
	//通过快照的最后一条索引+1来获取entries中的第一条索引
	if u.snapshot != nil {
		return u.snapshot.Metadata.Index + 1, true
	}
	return 0, false
}

// maybeLastIndex returns the last index if it has at least one
// unstable entry or snapshot.
// 如果至少有一条entry或者快照，则返回最后一条索引值
func (u *unstable) maybeLastIndex() (uint64, bool) {
	//如果entries不为空，则直接获取entries中最后一条索引
	if l := len(u.entries); l != 0 {
		return u.offset + uint64(l) - 1, true
	}
	//如果entries为空，那么LastIndex就是快照的最后一条索引
	if u.snapshot != nil {
		return u.snapshot.Metadata.Index, true
	}
	//如果没有entries且没有快照，则返回失败
	return 0, false
}

// maybeTerm returns the term of the entry at index i, if there
// is any.
// 通过索引值i，查找Term值
func (u *unstable) maybeTerm(i uint64) (uint64, bool) {
	//如果i小于offset，则在快照中查找
	if i < u.offset {
		//如果存在快照，且快照的索引值刚好等于i，则返回快照元数据的Term
		if u.snapshot != nil && u.snapshot.Metadata.Index == i {
			return u.snapshot.Metadata.Term, true
		}
		return 0, false
	}

	//如果i大于offset
	//获取lastIndex
	last, ok := u.maybeLastIndex()
	// 获取失败
	if !ok {
		return 0, false
	}
	//获取成功，但是i超出了unstable的已知范围，返回失败
	if i > last {
		return 0, false
	}

	//返回entries中，对应位置的Term值
	return u.entries[i-u.offset].Term, true
}

//unstable.entries中的Entry写入Storage后，会调用stableTo()方法，清除entries中对应的Entry记录
func (u *unstable) stableTo(i, t uint64) {
	//查找指定Entry的Term值，如果查找失败，则表示对应的Entry不在unstable中，直接返回
	gt, ok := u.maybeTerm(i)
	if !ok {
		return
	}
	// if i < offset, term is matched with the snapshot
	// only update the unstable entries if term is matched with
	// an unstable entry.
	if gt == t && i >= u.offset {
		u.entries = u.entries[i+1-u.offset:]
		u.offset = i + 1
		u.shrinkEntriesArray()
	}
}

// shrinkEntriesArray discards the underlying array used by the entries slice
// if most of it isn't being used. This avoids holding references to a bunch of
// potentially large entries that aren't needed anymore. Simply clearing the
// entries wouldn't be safe because clients might still be using them.
// append以及截断，会使得entries的容量越来越大，而entries实际使用的长度却很小，
// 当实际使用长度的2倍都还小于容量时，则新建一个合适大小的数组来保存entries
func (u *unstable) shrinkEntriesArray() {
	// We replace the array if we're using less than half of the space in
	// it. This number is fairly arbitrary, chosen as an attempt to balance
	// memory usage vs number of allocations. It could probably be improved
	// with some focused tuning.
	const lenMultiple = 2
	if len(u.entries) == 0 {
		u.entries = nil
	} else if len(u.entries)*lenMultiple < cap(u.entries) {
		newEntries := make([]pb.Entry, len(u.entries))
		copy(newEntries, u.entries)
		u.entries = newEntries
	}
}

//清理index=i 对应的快照.需要将快照写入storage后再执行该操作
func (u *unstable) stableSnapTo(i uint64) {
	if u.snapshot != nil && u.snapshot.Metadata.Index == i {
		u.snapshot = nil
	}
}

//将参数中的快照写入unstable
func (u *unstable) restore(s pb.Snapshot) {
	u.offset = s.Metadata.Index + 1
	u.entries = nil
	u.snapshot = &s
}

//截断与追加
// QUESTION: after > u.maybeLastIndex，这种情况未做处理，
// 直接进入default,导致程序出错
func (u *unstable) truncateAndAppend(ents []pb.Entry) {
	after := ents[0].Index
	switch {
	//如果传入的ents刚好能和u.entries无缝对接，在u.entries后追加ents
	case after == u.offset+uint64(len(u.entries)):
		// after is the next index in the u.entries
		// directly append
		u.entries = append(u.entries, ents...)
	//传入的ents比u.entries靠前,则替换u.entries 为ents
	case after <= u.offset:
		u.logger.Infof("replace the unstable entries from index %d", after)
		// The log is being truncated to before our current offset
		// portion, so set the offset and replace the entries
		u.offset = after
		u.entries = ents
	//传入的ents和u.entries有重合的部分,则去除重合部分，再追加写入后续的。
	default:
		// truncate to after and copy to u.entries
		// then append
		u.logger.Infof("truncate the unstable entries before index %d", after)
		u.entries = append([]pb.Entry{}, u.slice(u.offset, after)...)
		u.entries = append(u.entries, ents...)
	}
}

//切片，返回[lo:hi]的切片
func (u *unstable) slice(lo uint64, hi uint64) []pb.Entry {
	//边界检查
	u.mustCheckOutOfBounds(lo, hi)
	return u.entries[lo-u.offset : hi-u.offset]
}

// u.offset <= lo <= hi <= u.offset+len(u.entries)
func (u *unstable) mustCheckOutOfBounds(lo, hi uint64) {
	if lo > hi {
		u.logger.Panicf("invalid unstable.slice %d > %d", lo, hi)
	}
	upper := u.offset + uint64(len(u.entries))
	if lo < u.offset || hi > upper {
		u.logger.Panicf("unstable.slice[%d,%d) out of bound [%d,%d]", lo, hi, u.offset, upper)
	}
}
