// Copyright 2015 Felipe A. Cavani. All rights reserved.
// Use of this source code is governed by the Apache License 2.0
// license that can be found in the LICENSE file.

package boltdbutils

import (
	"bytes"
	"sync"

	"github.com/boltdb/bolt"
	"github.com/fcavani/e"
)

type Cursor struct {
	Tx          *bolt.Tx
	Bucket      []byte
	NumKeys     int
	Reverse     bool
	lck         sync.Mutex
	err         error
	cursors     []*bolt.Cursor
	cursorsSave []*bolt.Cursor
	// actual keys under the cursor
	ks       [][]byte
	ksSave   [][]byte
	rollback bool
	skip     [][]byte
	ls       int
}

func (c *Cursor) Init(keys ...[]byte) error {
	c.cursors = make([]*bolt.Cursor, c.NumKeys)
	c.ks = make([][]byte, c.NumKeys)
	c.cursorsSave = make([]*bolt.Cursor, c.NumKeys)
	c.ksSave = make([][]byte, c.NumKeys)

	for i := 0; i < c.NumKeys; i++ {
		c.cursorsSave[i] = new(bolt.Cursor)
	}

	b := c.Tx.Bucket(c.Bucket)
	if b == nil {
		return e.New(ErrInvBucket)
	}
	c.cursors[0] = b.Cursor()

	if len(keys) > c.NumKeys-1 {
		return e.New("invalid number of keys")
	}

	for i, key := range keys {
		c.ks[i] = key
		k, v := c.cursors[i].Seek(key)
		if k == nil {
			return e.New("key not found")
		}
		if !bytes.Equal(k, key) {
			return e.New("key not found")
		}
		if i+1 < c.NumKeys {
			c.cursors[i+1] = c.Tx.Bucket(v).Cursor()
		}
	}
	c.skip = keys
	c.ls = len(keys)
	return nil
}

func (c *Cursor) GetTx() *bolt.Tx {
	return c.Tx
}

const ErrInvBucket = "invalid bucket"

func (c *Cursor) Skip(count uint64) (k [][]byte, v []byte) {
	c.lck.Lock()
	defer c.lck.Unlock()

	c.saveState()
	defer func() {
		if k == nil {
			c.restoreState()
		}
	}()

	if c.Reverse {
		k, v = c.skipBackward(count)
		return
	}
	k, v = c.skipForward(count)
	return
}

func (c *Cursor) skipBackward(count uint64) ([][]byte, []byte) {
	var i uint64
	// Start a vector with all cursor set to start.
	for i := 1 + c.ls; i < c.NumKeys; i++ {
		k, v := c.cursors[i-1].Last()
		if v == nil {
			return nil, nil
		}
		c.cursors[i] = c.Tx.Bucket(v).Cursor()
		c.ks[i-1] = k
	}

	// Pick the last cursor to start counting.
	level := len(c.cursors) - 1
	p := c.cursors[level]
F:
	for {
		// Do counting.
		for k, v := p.Last(); k != nil; k, v = p.Prev() {
			if i == count {
				c.ks[level] = k
				return c.ks, v
			}
			i++
		}
	G:
		for i := level - 1; i >= c.ls; i-- {
			// Next in the prev level.
			k, v := c.cursors[i].Prev()
			if v == nil {
				if i == 0 {
					//no more entries in the last leval, stop the loop.
					break F
				}
				// No more entries in this level go prev.
				continue G
			}
			c.ks[i] = k

			// Update all c.cursors (cursors) from i + 1 to the end.
			for j := i + 1; j < c.NumKeys; j++ {
				// Update c.cursors with the new cursor.
				c.cursors[j] = c.Tx.Bucket(v).Cursor()
				// If not  the last catch the next and iterate
				if j < c.NumKeys-1 {
					k, v := c.cursors[j].Prev()
					if v == nil {
						c.err = e.Push(e.New("during the iteration found a entry that wasn't deleted"), e.New("error iterating over the data"))
						return nil, nil
					}
					c.ks[j] = k
				}
			}

			p = c.Tx.Bucket(v).Cursor()

			break
		}
	}
	return nil, nil
}

func (c *Cursor) skipForward(count uint64) ([][]byte, []byte) {
	var i uint64
	// Start a vector with all cursor set to start.
	for i := 1 + c.ls; i < c.NumKeys; i++ {
		k, v := c.cursors[i-1].First()
		if v == nil {
			return nil, nil
		}
		c.cursors[i] = c.Tx.Bucket(v).Cursor()
		c.ks[i-1] = k
	}

	// Pick the last cursor to start counting.
	level := len(c.cursors) - 1
	p := c.cursors[level]
F:
	for {
		// Do counting.
		for k, v := p.First(); k != nil; k, v = p.Next() {
			if i == count {
				c.ks[level] = k
				return c.ks, v
			}
			i++
		}
	G:
		for i := level - 1; i >= c.ls; i-- {
			// Next in the prev level.
			k, v := c.cursors[i].Next()
			if v == nil {
				if i == 0 {
					//no more entries in the last leval, stop the loop.
					break F
				}
				// No more entries in this level go prev.
				continue G
			}
			c.ks[i] = k

			// Update all c.cursors (cursors) from i + 1 to the end.
			for j := i + 1; j < c.NumKeys; j++ {
				// Update c.cursors with the new cursor.
				c.cursors[j] = c.Tx.Bucket(v).Cursor()
				// If not  the last catch the next and iterate
				if j < c.NumKeys-1 {
					k, v := c.cursors[j].Next()
					if v == nil {
						c.err = e.Push(e.New("during the iteration found a entry that wasn't deleted"), e.New("error iterating over the data"))
						return nil, nil
					}
					c.ks[j] = k
				}
			}

			p = c.Tx.Bucket(v).Cursor()

			break
		}
	}
	return nil, nil
}

func (c *Cursor) Seek(keys ...[]byte) (kout [][]byte, vout []byte) {
	c.lck.Lock()
	defer c.lck.Unlock()

	c.saveState()
	defer func() {
		if kout == nil {
			c.restoreState()
		}
	}()

	kout, vout = c.seek(keys...)
	return
}

func (c *Cursor) seek(keys ...[]byte) ([][]byte, []byte) {
	if len(keys) != c.NumKeys {
		c.err = e.New("wrong number of keys")
		return nil, nil
	}

	// TODO: check the semantics of Seek. This must return nil in some
	// point.

	for i, s := range c.skip {
		keys[i] = s
	}

	var k, v []byte
	for i := c.ls; i < c.NumKeys; i++ {
		k, v = c.cursors[i].Seek(keys[i])
		if k == nil {
			if i-1 < 0 {
				return nil, nil
			}
			if c.Reverse {
				if len(c.skip) > 0 && bytes.Compare(keys[i], c.ks[i]) == 1 {
					return c.next()
				}
				k, v = c.cursors[i].Last()
				if k == nil {
					return nil, nil
				}
				c.ks[i] = k
				if c.NumKeys-1 > i {
					c.cursors[i+1] = c.Tx.Bucket(v).Cursor()
					return c.forwardNext(i + 1)
				}
				return c.ks, v
			}

			if len(c.skip) > 0 && bytes.Compare(keys[i], c.ks[i]) == 1 {
				return c.last()
			}

			return c.backNext(i - 1)
		}
		c.ks[i] = k
		if c.NumKeys-1 > i {
			c.cursors[i+1] = c.Tx.Bucket(v).Cursor()
		}
	}
	return c.ks, v
}

func (c *Cursor) Next() (kout [][]byte, vout []byte) {
	c.lck.Lock()
	defer c.lck.Unlock()

	c.saveState()
	defer func() {
		if kout == nil {
			c.restoreState()
		}
	}()

	kout, vout = c.next()
	return
}

func (c *Cursor) next() ([][]byte, []byte) {
	level := len(c.cursors) - 1
	if c.cursors[level] == nil {
		return c.nextBack(level)
	}
	// Find next
	k, v := c.nextRev(level)
	if k != nil {
		c.ks[level] = k
		return c.ks, v
	}
	// Didn't find next go to prev level
	if level-1 >= c.ls {
		return c.backNext(level - 1)
	}
	return nil, nil
}

func (c *Cursor) Prev() (kout [][]byte, vout []byte) {
	c.lck.Lock()
	defer c.lck.Unlock()

	c.saveState()
	defer func() {
		if kout == nil {
			c.restoreState()
		}
	}()

	kout, vout = c.prev()
	return
}

func (c *Cursor) prev() ([][]byte, []byte) {
	level := len(c.cursors) - 1
	// Find next
	k, v := c.prevRev(level)
	if k != nil {
		c.ks[level] = k
		return c.ks, v
	}
	// Didn't find next go to prev level
	if level-1 >= c.ls {
		return c.backPrev(level - 1)
	}
	return nil, nil
}

func (c *Cursor) First() (kout [][]byte, vout []byte) {
	c.lck.Lock()
	defer c.lck.Unlock()

	c.saveState()
	defer func() {
		if kout == nil {
			c.restoreState()
		}
	}()

	var k, v []byte
	// Start a vector with all cursors set to start.
	for i := c.ls; i < c.NumKeys; i++ {
		k, v = c.firstRev(i)
		if k == nil {
			return
		}
		c.ks[i] = k
		if i+1 < c.NumKeys {
			c.cursors[i+1] = c.Tx.Bucket(v).Cursor()
		}
	}

	kout, vout = c.ks, v
	return
}

func (c *Cursor) Last() (kout [][]byte, vout []byte) {
	c.lck.Lock()
	defer c.lck.Unlock()

	c.saveState()
	defer func() {
		if kout == nil {
			c.restoreState()
		}
	}()

	kout, vout = c.last()
	return
}

func (c *Cursor) last() ([][]byte, []byte) {
	var k, v []byte
	// Start a vector with all cursor set to start.
	for i := c.ls; i < c.NumKeys; i++ {
		k, v = c.lastRev(i)
		if k == nil {
			return nil, nil
		}
		c.ks[i] = k
		if i+1 < c.NumKeys {
			c.cursors[i+1] = c.Tx.Bucket(v).Cursor()
		}
	}

	return c.ks, v
}

func (c *Cursor) Err() error {
	c.lck.Lock()
	defer c.lck.Unlock()

	err := c.err
	c.err = nil
	return err
}

func (c *Cursor) Commit() error {
	c.lck.Lock()
	defer c.lck.Unlock()

	if c.rollback {
		return e.New("already rolled back/commited")
	}
	if c.Tx.Writable() {
		err := c.Tx.Commit()
		if err != nil {
			return e.Forward(err)
		}
		c.rollback = true
		return nil
	}
	err := c.Tx.Rollback()
	if err != nil {
		return e.Forward(err)
	}
	c.rollback = true
	return nil
}

func (c *Cursor) Rollback() error {
	c.lck.Lock()
	defer c.lck.Unlock()

	if c.rollback {
		return e.New("already rolled back/commited")
	}

	err := c.Tx.Rollback()
	if err != nil {
		return e.Forward(err)
	}
	c.rollback = true
	return nil
}

func (c *Cursor) firstRev(i int) ([]byte, []byte) {
	if c.Reverse {
		return c.cursors[i].Last()
	}
	return c.cursors[i].First()
}

func (c *Cursor) lastRev(i int) ([]byte, []byte) {
	if c.Reverse {
		return c.cursors[i].First()
	}
	return c.cursors[i].Last()
}

func (c *Cursor) backNext(i int) ([][]byte, []byte) {
	k, v := c.nextRev(i)
	if k == nil {
		if i == c.ls {
			return nil, nil
		}
		if i-1 < 0 {
			return nil, nil
		}
		return c.backNext(i - 1)
	}
	c.ks[i] = k
	if i+1 < c.NumKeys {
		c.cursors[i+1] = c.Tx.Bucket(v).Cursor()
		return c.forwardNext(i + 1)
	}
	return c.ks, v
}

func (c *Cursor) backPrev(i int) ([][]byte, []byte) {
	k, v := c.prevRev(i)
	if k == nil {
		if i == c.ls {
			return nil, nil
		}
		if i-1 < 0 {
			return nil, nil
		}
		return c.backPrev(i - 1)
	}
	c.ks[i] = k
	if i+1 < c.NumKeys {
		c.cursors[i+1] = c.Tx.Bucket(v).Cursor()
		return c.forwardPrev(i + 1)
	}
	return c.ks, v
}

func (c *Cursor) forwardNext(i int) ([][]byte, []byte) {
	k, v := c.firstRev(i)
	if k == nil {
		if i == c.ls {
			return nil, nil
		}
		c.err = e.New("db error")
		return nil, nil
	}
	c.ks[i] = k
	if i+1 < c.NumKeys {
		c.cursors[i+1] = c.Tx.Bucket(v).Cursor()
		return c.forwardNext(i + 1)
	}
	return c.ks, v
}

func (c *Cursor) forwardPrev(i int) ([][]byte, []byte) {
	k, v := c.lastRev(i)
	if k == nil {
		if i == c.ls {
			return nil, nil
		}
		c.err = e.New("db error")
		return nil, nil
	}
	c.ks[i] = k
	if i+1 < c.NumKeys {
		c.cursors[i+1] = c.Tx.Bucket(v).Cursor()
		return c.forwardPrev(i + 1)
	}
	return c.ks, v

}

func (c *Cursor) nextRev(i int) ([]byte, []byte) {
	if c.Reverse {
		return c.cursors[i].Prev()
	}
	return c.cursors[i].Next()
}

func (c *Cursor) prevRev(i int) ([]byte, []byte) {
	if c.Reverse {
		return c.cursors[i].Next()
	}
	return c.cursors[i].Prev()
}

func (c *Cursor) nextForward(i int) ([][]byte, []byte) {
	k, v := c.cursors[i].Next()
	if k == nil {
		return nil, nil
	}
	c.ks[i] = k
	if i < c.NumKeys-1 {
		return c.nextForward(i + 1)
	}
	return c.ks, v
}

func (c *Cursor) nextBack(i int) ([][]byte, []byte) {
	if i-1 < 0 {
		return nil, nil
	}
	if c.cursors[i] != nil {
		k, v := c.nextRev(i)
		if k == nil {
			return nil, nil
		}
		c.ks[i] = k
		c.cursors[i+1] = c.Tx.Bucket(v).Cursor()
		if i < c.NumKeys-1 {
			return c.nextForward(i + 1)
		}
		return c.ks, v
	}
	return c.nextBack(i - 1)
}

func (c *Cursor) saveState() {
	for i := 0; i < len(c.cursors); i++ {
		if c.cursors[i] == nil {
			continue
		}
		*c.cursorsSave[i] = *c.cursors[i]
		copy(c.ksSave[i], c.ks[i])
	}
}

func (c *Cursor) restoreState() {
	for i := 0; i < len(c.cursors); i++ {
		*c.cursors[i] = *c.cursorsSave[i]
		copy(c.ks[i], c.ksSave[i])
	}
}
