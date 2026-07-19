package zset_storage

import (
	"math/rand"
	"time"
)

const (
	zskiplistMaxLevel = 32
	zskiplistP        = 0.25
)

type zskiplistNode struct {
	ele      string
	score    float64
	backward *zskiplistNode
	level    []zskiplistLevel
}

type zskiplistLevel struct {
	forward *zskiplistNode
	span    uint64
}

type zskiplist struct {
	header, tail *zskiplistNode
	length       uint64
	level        int
	randSource   *rand.Rand
}

func newZskiplistNode(level int, score float64, ele string) *zskiplistNode {
	return &zskiplistNode{
		ele:   ele,
		score: score,
		level: make([]zskiplistLevel, level),
	}
}

func newZskiplist() *zskiplist {
	src := rand.NewSource(time.Now().UnixNano())
	return &zskiplist{
		header:     newZskiplistNode(zskiplistMaxLevel, 0, ""),
		level:      1,
		randSource: rand.New(src),
	}
}

func (zsl *zskiplist) randomLevel() int {
	level := 1
	for zsl.randSource.Float64() < zskiplistP && level < zskiplistMaxLevel {
		level++
	}
	return level
}

func (zsl *zskiplist) insert(score float64, ele string) *zskiplistNode {
	update := make([]*zskiplistNode, zskiplistMaxLevel)
	rank := make([]uint64, zskiplistMaxLevel)

	x := zsl.header
	for i := zsl.level - 1; i >= 0; i-- {
		if i == zsl.level-1 {
			rank[i] = 0
		} else {
			rank[i] = rank[i+1]
		}
		for x.level[i].forward != nil &&
			(x.level[i].forward.score < score ||
				(x.level[i].forward.score == score && x.level[i].forward.ele < ele)) {
			rank[i] += x.level[i].span
			x = x.level[i].forward
		}
		update[i] = x
	}

	level := zsl.randomLevel()
	if level > zsl.level {
		for i := zsl.level; i < level; i++ {
			rank[i] = 0
			update[i] = zsl.header
			update[i].level[i].span = zsl.length
		}
		zsl.level = level
	}

	x = newZskiplistNode(level, score, ele)
	for i := 0; i < level; i++ {
		x.level[i].forward = update[i].level[i].forward
		update[i].level[i].forward = x

		x.level[i].span = update[i].level[i].span - (rank[0] - rank[i])
		update[i].level[i].span = (rank[0] - rank[i]) + 1
	}

	for i := level; i < zsl.level; i++ {
		update[i].level[i].span++
	}

	if update[0] == zsl.header {
		x.backward = nil
	} else {
		x.backward = update[0]
	}

	if x.level[0].forward != nil {
		x.level[0].forward.backward = x
	} else {
		zsl.tail = x
	}

	zsl.length++
	return x
}

func (zsl *zskiplist) deleteNode(x *zskiplistNode, update []*zskiplistNode) {
	for i := 0; i < zsl.level; i++ {
		if update[i].level[i].forward == x {
			update[i].level[i].span += x.level[i].span - 1
			update[i].level[i].forward = x.level[i].forward
		} else {
			update[i].level[i].span--
		}
	}
	if x.level[0].forward != nil {
		x.level[0].forward.backward = x.backward
	} else {
		zsl.tail = x.backward
	}
	for zsl.level > 1 && zsl.header.level[zsl.level-1].forward == nil {
		zsl.level--
	}
	zsl.length--
}

func (zsl *zskiplist) delete(score float64, ele string) bool {
	update := make([]*zskiplistNode, zskiplistMaxLevel)
	x := zsl.header
	for i := zsl.level - 1; i >= 0; i-- {
		for x.level[i].forward != nil &&
			(x.level[i].forward.score < score ||
				(x.level[i].forward.score == score && x.level[i].forward.ele < ele)) {
			x = x.level[i].forward
		}
		update[i] = x
	}
	x = x.level[0].forward
	if x != nil && score == x.score && x.ele == ele {
		zsl.deleteNode(x, update)
		return true
	}
	return false
}

func (zsl *zskiplist) getRank(score float64, ele string) uint64 {
	var rank uint64
	x := zsl.header
	for i := zsl.level - 1; i >= 0; i-- {
		for x.level[i].forward != nil &&
			(x.level[i].forward.score < score ||
				(x.level[i].forward.score == score && x.level[i].forward.ele <= ele)) {
			rank += x.level[i].span
			x = x.level[i].forward
		}
		if x != zsl.header && x.ele == ele {
			return rank
		}
	}
	return 0
}

func (zsl *zskiplist) getElementByRank(rank uint64) *zskiplistNode {
	if rank == 0 || rank > zsl.length {
		return nil
	}
	x := zsl.header
	var traversed uint64
	for i := zsl.level - 1; i >= 0; i-- {
		for x.level[i].forward != nil && traversed+x.level[i].span <= rank {
			traversed += x.level[i].span
			x = x.level[i].forward
		}
		if traversed == rank {
			return x
		}
	}
	return nil
}
