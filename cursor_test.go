// Copyright 2015 Felipe A. Cavani. All rights reserved.
// Use of this source code is governed by the Apache License 2.0
// license that can be found in the LICENSE file.

package boltdbutils

import (
	"bytes"
	"encoding/binary"
	"io/ioutil"
	"path/filepath"
	"testing"

	"github.com/boltdb/bolt"
	"github.com/fcavani/e"
	"github.com/fcavani/rand"
)

func TestCursorSkipNobucket(t *testing.T) {
	filename, err := rand.FileName("blog-", "db", 10)
	if err != nil {
		t.Fatal(e.Trace(e.Forward(err)))
	}

	dir, err := ioutil.TempDir("", "blog-")
	if err != nil {
		t.Fatal(e.Trace(e.Forward(err)))
	}

	db, err := bolt.Open(filepath.Join(dir, filename), 0600, nil)
	if err != nil {
		t.Fatal(e.Trace(e.Forward(err)))
	}

	err = db.View(func(tx *bolt.Tx) error {
		c := &Cursor{
			Tx:      tx,
			Bucket:  []byte("test_bucket"),
			NumKeys: 3,
		}
		err := c.Init()
		if err != nil {
			return e.Forward(err)
		}
		c.Skip(3)
		if err := c.Err(); err != nil {
			return e.Forward(err)
		}
		return nil
	})
	if err != nil && !e.Equal(err, ErrInvBucket) {
		t.Fatal(e.Trace(e.Forward(err)))
	}
}

func skip(n uint64, c *Cursor, data []testData) error {
	keys, val := c.Skip(n)
	if err := c.Err(); err != nil {
		return e.Forward(err)
	}
	if keys == nil {
		return e.New("skip returned nil")
	}
	if !bytes.Equal(val, data[int(n)].Data) {
		return e.New("not equal %v", string(val))
	}
	for i, key := range keys {
		if !bytes.Equal(key, data[n].Keys[i]) {
			return e.New("key is not equal %v %v", string(key), string(data[n].Keys[i]))
		}
	}
	return nil
}

func TestCursorSkip(t *testing.T) {
	data := []testData{
		{[]byte("test_bucket"), [][]byte{[]byte("key1"), []byte("key1")}, []byte("11")},
		{[]byte("test_bucket"), [][]byte{[]byte("key1"), []byte("key2")}, []byte("12")},
		{[]byte("test_bucket"), [][]byte{[]byte("key1"), []byte("key3")}, []byte("13")},
		{[]byte("test_bucket"), [][]byte{[]byte("key2"), []byte("key1")}, []byte("21")},
		{[]byte("test_bucket"), [][]byte{[]byte("key2"), []byte("key2")}, []byte("22")},
		{[]byte("test_bucket"), [][]byte{[]byte("key3"), []byte("key1")}, []byte("31")},
	}

	filename, err := rand.FileName("blog-", "db", 10)
	if err != nil {
		t.Fatal(e.Trace(e.Forward(err)))
	}

	dir, err := ioutil.TempDir("", "blog-")
	if err != nil {
		t.Fatal(e.Trace(e.Forward(err)))
	}

	db, err := bolt.Open(filepath.Join(dir, filename), 0600, nil)
	if err != nil {
		t.Fatal(e.Trace(e.Forward(err)))
	}

	err = db.Update(func(tx *bolt.Tx) error {
		for i, d := range data {
			err := Put(tx, d.Bucket, d.Keys, d.Data)
			if err != nil {
				return e.Push(err, e.New("Fail to put %v", i))
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(e.Trace(e.Forward(err)))
	}

	err = db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("test_bucket"))
		buf := b.Get([]byte("key1"))
		if buf == nil {
			return e.New("key not found")
		}
		return nil
	})
	if err != nil {
		t.Fatal(e.Trace(e.Forward(err)))
	}

	err = db.View(func(tx *bolt.Tx) error {
		c := &Cursor{
			Tx:      tx,
			Bucket:  []byte("test_bucket"),
			NumKeys: 2,
		}
		err := c.Init()
		if err != nil {
			return e.Forward(err)
		}

		var i uint64
		for ; i < uint64(len(data))-1; i++ {
			err := skip(i, c, data)
			if err != nil {
				return e.Forward(err)
			}
		}

		return nil
	})
	if err != nil {
		t.Fatal(e.Trace(e.Forward(err)))
	}

}

func TestCursorSeek(t *testing.T) {
	data := []testData{
		{[]byte("test_bucket"), [][]byte{[]byte("key1"), []byte("key1")}, []byte("11")},
		{[]byte("test_bucket"), [][]byte{[]byte("key1"), []byte("key2")}, []byte("12")},
		{[]byte("test_bucket"), [][]byte{[]byte("key1"), []byte("key3")}, []byte("13")},
		{[]byte("test_bucket"), [][]byte{[]byte("key2"), []byte("key1")}, []byte("21")},
		{[]byte("test_bucket"), [][]byte{[]byte("key2"), []byte("key2")}, []byte("22")},
		{[]byte("test_bucket"), [][]byte{[]byte("key3"), []byte("key1")}, []byte("31")},
	}

	filename, err := rand.FileName("blog-", "db", 10)
	if err != nil {
		t.Fatal(e.Trace(e.Forward(err)))
	}

	dir, err := ioutil.TempDir("", "blog-")
	if err != nil {
		t.Fatal(e.Trace(e.Forward(err)))
	}

	db, err := bolt.Open(filepath.Join(dir, filename), 0600, nil)
	if err != nil {
		t.Fatal(e.Trace(e.Forward(err)))
	}

	err = db.Update(func(tx *bolt.Tx) error {
		for i, d := range data {
			err := Put(tx, d.Bucket, d.Keys, d.Data)
			if err != nil {
				return e.Push(err, e.New("Fail to put %v", i))
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(e.Trace(e.Forward(err)))
	}

	err = db.View(func(tx *bolt.Tx) error {
		c := &Cursor{
			Tx:      tx,
			Bucket:  []byte("test_bucket"),
			NumKeys: 2,
		}
		err := c.Init()
		if err != nil {
			return e.Forward(err)
		}
		for i, d := range data {
			k, v := c.Seek(d.Keys...)
			if k == nil {
				return e.New("key not found")
			}
			for j, key := range k {
				if !bytes.Equal(key, data[i].Keys[j]) {
					return e.New("key is not equal %v %v %v", i, string(key), string(data[i].Keys[j]))
				}
			}
			if !bytes.Equal(v, data[i].Data) {
				return e.New("not equal %v", string(v))
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(e.Trace(e.Forward(err)))
	}

	data2 := []testData{
		{[]byte("test_bucket"), [][]byte{[]byte("key1"), []byte("key1")}, []byte("11")},
		{[]byte("test_bucket"), [][]byte{[]byte("key1"), []byte("key4")}, []byte("21")},
		{[]byte("test_bucket"), [][]byte{[]byte("key2"), []byte("key3")}, []byte("31")},
		{[]byte("test_bucket"), [][]byte{[]byte("key4"), []byte("key1")}, nil},
	}

	err = db.View(func(tx *bolt.Tx) error {
		c := &Cursor{
			Tx:      tx,
			Bucket:  []byte("test_bucket"),
			NumKeys: 2,
		}
		err := c.Init()
		if err != nil {
			return e.Forward(err)
		}
		for i, d := range data2 {
			k, v := c.Seek(d.Keys...)
			if k == nil && d.Data == nil {
				continue
			} else if k == nil && d.Data != nil {
				return e.New("key not found %v", i)
			}
			if !bytes.Equal(v, data2[i].Data) {
				return e.New("not equal %v %v %v", i, string(v), string(data2[i].Data))
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(e.Trace(e.Forward(err)))
	}

}

func TestCursorFirstLast(t *testing.T) {
	data := []testData{
		{[]byte("test_bucket"), [][]byte{[]byte("key1"), []byte("key1")}, []byte("11")},
		{[]byte("test_bucket"), [][]byte{[]byte("key1"), []byte("key2")}, []byte("12")},
		{[]byte("test_bucket"), [][]byte{[]byte("key1"), []byte("key3")}, []byte("13")},
		{[]byte("test_bucket"), [][]byte{[]byte("key2"), []byte("key1")}, []byte("21")},
		{[]byte("test_bucket"), [][]byte{[]byte("key2"), []byte("key2")}, []byte("22")},
		{[]byte("test_bucket"), [][]byte{[]byte("key3"), []byte("key1")}, []byte("31")},
	}

	filename, err := rand.FileName("blog-", "db", 10)
	if err != nil {
		t.Fatal(e.Trace(e.Forward(err)))
	}

	dir, err := ioutil.TempDir("", "blog-")
	if err != nil {
		t.Fatal(e.Trace(e.Forward(err)))
	}

	db, err := bolt.Open(filepath.Join(dir, filename), 0600, nil)
	if err != nil {
		t.Fatal(e.Trace(e.Forward(err)))
	}

	err = db.Update(func(tx *bolt.Tx) error {
		for i, d := range data {
			err := Put(tx, d.Bucket, d.Keys, d.Data)
			if err != nil {
				return e.Push(err, e.New("Fail to put %v", i))
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(e.Trace(e.Forward(err)))
	}

	err = db.View(func(tx *bolt.Tx) error {
		c := &Cursor{
			Tx:      tx,
			Bucket:  []byte("test_bucket"),
			NumKeys: 2,
		}
		err := c.Init()
		if err != nil {
			return e.Forward(err)
		}

		keys, val := c.First()
		if keys == nil {
			return e.New("First returned nil")
		}
		if !bytes.Equal(val, data[0].Data) {
			return e.New("not equal %v %v", string(val), string(data[0].Data))
		}
		for i, key := range keys {
			if !bytes.Equal(key, data[0].Keys[i]) {
				return e.New("key is not equal %v %v", string(key), string(data[0].Keys[i]))
			}
		}

		keys, val = c.Last()
		if keys == nil {
			return e.New("First returned nil")
		}
		l := len(data) - 1
		if !bytes.Equal(val, data[l].Data) {
			return e.New("not equal %v", string(val))
		}
		for i, key := range keys {
			if !bytes.Equal(key, data[l].Keys[i]) {
				return e.New("key is not equal %v %v", string(key), string(data[l].Keys[i]))
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(e.Trace(e.Forward(err)))
	}
}

func TestCursorNext(t *testing.T) {
	data := []testData{
		{[]byte("test_bucket"), [][]byte{[]byte("key1"), []byte("key1")}, []byte("11")},
		{[]byte("test_bucket"), [][]byte{[]byte("key1"), []byte("key2")}, []byte("12")},
		{[]byte("test_bucket"), [][]byte{[]byte("key1"), []byte("key3")}, []byte("13")},
		{[]byte("test_bucket"), [][]byte{[]byte("key2"), []byte("key1")}, []byte("21")},
		{[]byte("test_bucket"), [][]byte{[]byte("key2"), []byte("key2")}, []byte("22")},
		{[]byte("test_bucket"), [][]byte{[]byte("key3"), []byte("key1")}, []byte("31")},
	}

	filename, err := rand.FileName("blog-", "db", 10)
	if err != nil {
		t.Fatal(e.Trace(e.Forward(err)))
	}

	dir, err := ioutil.TempDir("", "blog-")
	if err != nil {
		t.Fatal(e.Trace(e.Forward(err)))
	}

	db, err := bolt.Open(filepath.Join(dir, filename), 0600, nil)
	if err != nil {
		t.Fatal(e.Trace(e.Forward(err)))
	}

	err = db.Update(func(tx *bolt.Tx) error {
		for i, d := range data {
			err := Put(tx, d.Bucket, d.Keys, d.Data)
			if err != nil {
				return e.Push(err, e.New("Fail to put %v", i))
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(e.Trace(e.Forward(err)))
	}

	err = db.View(func(tx *bolt.Tx) error {
		c := &Cursor{
			Tx:      tx,
			Bucket:  []byte("test_bucket"),
			NumKeys: 2,
		}
		err := c.Init()
		if err != nil {
			return e.Forward(err)
		}
		i := 0
		for k, v := c.First(); k != nil; k, v = c.Next() {
			if !bytes.Equal(v, data[i].Data) {
				return e.New("not equal %v", string(v))
			}
			for j, key := range k {
				if !bytes.Equal(key, data[i].Keys[j]) {
					return e.New("key is not equal %v %v", string(key), string(data[i].Keys[j]))
				}
			}
			i++
		}
		return nil
	})
	if err != nil {
		t.Fatal(e.Trace(e.Forward(err)))
	}
}

func EncInt(x int) []byte {
	buf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutVarint(buf, int64(x))
	return buf[:n]
}

func TestCursorBigIndexNextPrev(t *testing.T) {
	data := []testData{
		{[]byte("test_bucket"), [][]byte{[]byte{'0'}, []byte("pt-br"), EncInt(2015), EncInt(1), EncInt(4), EncInt(14), EncInt(58), EncInt(59), []byte("Log")}, []byte("11")},
		{[]byte("test_bucket"), [][]byte{[]byte{'1'}, []byte("pt-br"), EncInt(2015), EncInt(12), EncInt(23), EncInt(17), EncInt(25), EncInt(59), []byte("Sem assunto e sem nome")}, []byte("12")},
	}

	filename, err := rand.FileName("blog-", "db", 10)
	if err != nil {
		t.Fatal(e.Trace(e.Forward(err)))
	}

	dir, err := ioutil.TempDir("", "blog-")
	if err != nil {
		t.Fatal(e.Trace(e.Forward(err)))
	}

	db, err := bolt.Open(filepath.Join(dir, filename), 0600, nil)
	if err != nil {
		t.Fatal(e.Trace(e.Forward(err)))
	}

	err = db.Update(func(tx *bolt.Tx) error {
		for i, d := range data {
			err := Put(tx, d.Bucket, d.Keys, d.Data)
			if err != nil {
				return e.Push(err, e.New("Fail to put %v", i))
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(e.Trace(e.Forward(err)))
	}

	err = db.View(func(tx *bolt.Tx) error {
		c := &Cursor{
			Tx:      tx,
			Bucket:  []byte("test_bucket"),
			NumKeys: 9,
		}
		err := c.Init()
		if err != nil {
			return e.Forward(err)
		}
		i := 0
		for k, v := c.First(); k != nil; k, v = c.Next() {
			if err := c.Err(); err != nil {
				return e.Forward(err)
			}
			if !bytes.Equal(v, data[i].Data) {
				return e.New("not equal %v %v", i, string(v))
			}
			for j, key := range k {
				if !bytes.Equal(key, data[i].Keys[j]) {
					return e.New("key is not equal %v %v %v", i, string(key), string(data[i].Keys[j]))
				}
			}
			i++
		}
		i = len(data) - 1
		for k, v := c.Last(); k != nil; k, v = c.Prev() {
			if err := c.Err(); err != nil {
				return e.Forward(err)
			}
			if !bytes.Equal(v, data[i].Data) {
				return e.New("not equal %v %v", i, string(v))
			}
			for j, key := range k {
				if !bytes.Equal(key, data[i].Keys[j]) {
					return e.New("key is not equal %v %v %v", i, string(key), string(data[i].Keys[j]))
				}
			}
			i--
		}
		return nil
	})
	if err != nil {
		t.Fatal(e.Trace(e.Forward(err)))
	}
}

func TestCursorBigIndexSeek(t *testing.T) {
	data := []testData{
		{[]byte("test_bucket"), [][]byte{[]byte{'0'}, []byte("pt-br"), EncInt(2015), EncInt(1), EncInt(4), EncInt(14), EncInt(58), EncInt(59), []byte("Log")}, []byte("11")},
		{[]byte("test_bucket"), [][]byte{[]byte{'1'}, []byte("pt-br"), EncInt(2015), EncInt(12), EncInt(23), EncInt(17), EncInt(25), EncInt(59), []byte("Sem assunto e sem nome")}, []byte("12")},
	}

	filename, err := rand.FileName("blog-", "db", 10)
	if err != nil {
		t.Fatal(e.Trace(e.Forward(err)))
	}

	dir, err := ioutil.TempDir("", "blog-")
	if err != nil {
		t.Fatal(e.Trace(e.Forward(err)))
	}

	db, err := bolt.Open(filepath.Join(dir, filename), 0600, nil)
	if err != nil {
		t.Fatal(e.Trace(e.Forward(err)))
	}

	err = db.Update(func(tx *bolt.Tx) error {
		for i, d := range data {
			err := Put(tx, d.Bucket, d.Keys, d.Data)
			if err != nil {
				return e.Push(err, e.New("Fail to put %v", i))
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(e.Trace(e.Forward(err)))
	}

	err = db.View(func(tx *bolt.Tx) error {
		c := &Cursor{
			Tx:      tx,
			Bucket:  []byte("test_bucket"),
			NumKeys: 9,
		}
		err := c.Init()
		if err != nil {
			return e.Forward(err)
		}
		_, v := c.Seek([]byte{'1'}, []byte("pt-br"), EncInt(2015), EncInt(12), EncInt(23), EncInt(17), EncInt(25), EncInt(59), []byte("Sem assunto e sem nome"))
		if !bytes.Equal(v, []byte("12")) {
			t.Fatal("seek fail", string(v))
		}
		t.Log(string(v))
		// _, v = c.Next()
		// if v != nil {
		// 	t.Fatal("next fail: not nil")
		// }
		_, v = c.Prev()
		if v == nil {
			t.Fatal("prev is nil")
		}
		if !bytes.Equal(v, []byte("11")) {
			t.Fatal("prev not equal", string(v))
		}
		_, v = c.Prev()
		if v != nil {
			t.Fatal("prev fail: not nil")
		}
		return nil
	})
	if err != nil {
		t.Fatal(e.Trace(e.Forward(err)))
	}
}

func TestCursorPrev(t *testing.T) {
	data := []testData{
		{[]byte("test_bucket"), [][]byte{[]byte("key1"), []byte("key1")}, []byte("11")},
		{[]byte("test_bucket"), [][]byte{[]byte("key1"), []byte("key2")}, []byte("12")},
		{[]byte("test_bucket"), [][]byte{[]byte("key1"), []byte("key3")}, []byte("13")},
		{[]byte("test_bucket"), [][]byte{[]byte("key2"), []byte("key1")}, []byte("21")},
		{[]byte("test_bucket"), [][]byte{[]byte("key2"), []byte("key2")}, []byte("22")},
		{[]byte("test_bucket"), [][]byte{[]byte("key3"), []byte("key1")}, []byte("31")},
	}

	filename, err := rand.FileName("blog-", "db", 10)
	if err != nil {
		t.Fatal(e.Trace(e.Forward(err)))
	}

	dir, err := ioutil.TempDir("", "blog-")
	if err != nil {
		t.Fatal(e.Trace(e.Forward(err)))
	}

	db, err := bolt.Open(filepath.Join(dir, filename), 0600, nil)
	if err != nil {
		t.Fatal(e.Trace(e.Forward(err)))
	}

	err = db.Update(func(tx *bolt.Tx) error {
		for i, d := range data {
			err := Put(tx, d.Bucket, d.Keys, d.Data)
			if err != nil {
				return e.Push(err, e.New("Fail to put %v", i))
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(e.Trace(e.Forward(err)))
	}

	err = db.View(func(tx *bolt.Tx) error {
		c := &Cursor{
			Tx:      tx,
			Bucket:  []byte("test_bucket"),
			NumKeys: 2,
		}
		err := c.Init()
		if err != nil {
			return e.Forward(err)
		}
		i := len(data) - 1
		for k, v := c.Last(); k != nil; k, v = c.Prev() {
			if !bytes.Equal(v, data[i].Data) {
				return e.New("not equal %v %v %v", i, string(v), string(data[i].Data))
			}
			for j, key := range k {
				if !bytes.Equal(key, data[i].Keys[j]) {
					return e.New("key is not equal %v %v", string(key), string(data[i].Keys[j]))
				}
			}
			i--
		}
		return nil
	})
	if err != nil {
		t.Fatal(e.Trace(e.Forward(err)))
	}
}

func TestCursorSkipKeys(t *testing.T) {
	data := []testData{
		{[]byte("test_bucket"), [][]byte{[]byte("key1"), []byte("key1")}, []byte("11")},
		{[]byte("test_bucket"), [][]byte{[]byte("key1"), []byte("key2")}, []byte("12")},
		{[]byte("test_bucket"), [][]byte{[]byte("key1"), []byte("key3")}, []byte("13")},
		{[]byte("test_bucket"), [][]byte{[]byte("key2"), []byte("key1")}, []byte("21")},
		{[]byte("test_bucket"), [][]byte{[]byte("key2"), []byte("key2")}, []byte("22")},
		{[]byte("test_bucket"), [][]byte{[]byte("key3"), []byte("key1")}, []byte("31")},
	}

	filename, err := rand.FileName("blog-", "db", 10)
	if err != nil {
		t.Fatal(e.Trace(e.Forward(err)))
	}

	dir, err := ioutil.TempDir("", "blog-")
	if err != nil {
		t.Fatal(e.Trace(e.Forward(err)))
	}

	db, err := bolt.Open(filepath.Join(dir, filename), 0600, nil)
	if err != nil {
		t.Fatal(e.Trace(e.Forward(err)))
	}

	err = db.Update(func(tx *bolt.Tx) error {
		for i, d := range data {
			err := Put(tx, d.Bucket, d.Keys, d.Data)
			if err != nil {
				return e.Push(err, e.New("Fail to put %v", i))
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(e.Trace(e.Forward(err)))
	}

	err = db.View(func(tx *bolt.Tx) error {
		c := &Cursor{
			Tx:      tx,
			Bucket:  []byte("test_bucket"),
			NumKeys: 2,
		}
		err := c.Init([]byte("key2"))
		if err != nil {
			return e.Forward(err)
		}

		//Next
		i := 3
		for k, v := c.First(); k != nil; k, v = c.Next() {
			if !bytes.Equal(v, data[i].Data) {
				return e.New("not equal %v", string(v))
			}
			for j, key := range k {
				if !bytes.Equal(key, data[i].Keys[j]) {
					return e.New("key is not equal %v %v", string(key), string(data[i].Keys[j]))
				}
			}
			i++
		}
		t.Log(i)
		if i > 5 {
			t.Fatal("iterator didn't stop", i)
		}

		//Prev
		i = 4
		for k, v := c.Last(); k != nil; k, v = c.Prev() {
			if !bytes.Equal(v, data[i].Data) {
				return e.New("not equal %v", string(v))
			}
			for j, key := range k {
				if !bytes.Equal(key, data[i].Keys[j]) {
					return e.New("key is not equal %v %v", string(key), string(data[i].Keys[j]))
				}
			}
			i--
		}
		t.Log(i)
		if i < 2 {
			t.Fatal("iterator didn't stop", i)
		}

		//Skip
		var m uint64
		var j int = 3
		for ; j < 5; j++ {
			keys, val := c.Skip(m)
			if err := c.Err(); err != nil {
				return e.Forward(err)
			}
			if keys == nil {
				return e.New("skip returned nil")
			}
			if !bytes.Equal(val, data[j].Data) {
				return e.New("not equal %v %v %v", m, string(val), string(data[j].Data))
			}
			for l, key := range keys {
				if !bytes.Equal(key, data[j].Keys[l]) {
					return e.New("key is not equal %v %v", string(key), string(data[j].Keys[l]))
				}
			}
			m++
		}

		//Seek
		for i, d := range data[3:5] {
			i = i + 3
			k, v := c.Seek(d.Keys...)
			if k == nil {
				return e.New("key not found")
			}
			for j, key := range k {
				if !bytes.Equal(key, data[i].Keys[j]) {
					return e.New("key is not equal %v %v %v", i, string(key), string(data[i].Keys[j]))
				}
			}
			if !bytes.Equal(v, data[i].Data) {
				return e.New("not equal %v", string(v))
			}
		}

		k, v := c.Seek(data[0].Keys...)
		if k == nil {
			return e.New("key not found")
		}
		t.Log(string(v))
		if !bytes.Equal(v, data[3].Data) {
			return e.New("not equal %v", string(v))
		}

		k, v = c.Seek([]byte("bú"), []byte("key3"))
		if k == nil {
			return e.New("key not found")
		}
		t.Log(string(v))
		if !bytes.Equal(v, data[4].Data) {
			return e.New("not equal %v", string(v))
		}

		return nil
	})
	if err != nil {
		t.Fatal(e.Trace(e.Forward(err)))
	}
}

func TestCursorReinsert(t *testing.T) {
	data := []testData{
		{[]byte("test_bucket"), [][]byte{[]byte{'0'}, []byte("pt-br"), EncInt(2015), EncInt(1), EncInt(4), EncInt(14), EncInt(58), EncInt(59), []byte("Log")}, []byte("11")},
		{[]byte("test_bucket"), [][]byte{[]byte{'1'}, []byte("pt-br"), EncInt(2015), EncInt(12), EncInt(23), EncInt(17), EncInt(25), EncInt(59), []byte("Sem assunto e sem nome")}, []byte("12")},
	}
	filename, err := rand.FileName("blog-", "db", 10)
	if err != nil {
		t.Fatal(e.Trace(e.Forward(err)))
	}

	dir, err := ioutil.TempDir("", "blog-")
	if err != nil {
		t.Fatal(e.Trace(e.Forward(err)))
	}

	db, err := bolt.Open(filepath.Join(dir, filename), 0600, nil)
	if err != nil {
		t.Fatal(e.Trace(e.Forward(err)))
	}
	err = db.Update(func(tx *bolt.Tx) error {
		for i, d := range data {
			err := Put(tx, d.Bucket, d.Keys, d.Data)
			if err != nil {
				return e.Push(err, e.New("Fail to put %v", i))
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(e.Trace(e.Forward(err)))
	}
	err = db.Update(func(tx *bolt.Tx) error {
		d := data[0]
		err := Del(tx, d.Bucket, d.Keys)
		if err != nil {
			return e.Forward(err)
		}
		err = Put(tx, d.Bucket, d.Keys, d.Data)
		if err != nil {
			return e.Forward(err)
		}
		return nil
	})
	if err != nil {
		t.Fatal(e.Trace(e.Forward(err)))
	}
	err = db.Update(func(tx *bolt.Tx) error {
		d := data[0]
		_, err := Get(tx, d.Bucket, d.Keys)
		if err != nil {
			return e.Forward(err)
		}
		c := &Cursor{
			Tx:      tx,
			Bucket:  []byte("test_bucket"),
			NumKeys: 9,
		}
		err = c.Init([]byte{'0'}, []byte("pt-br"))
		if err != nil {
			return e.Forward(err)
		}
		k, v := c.First()
		if k == nil {
			return e.New("can't get the first record")
		}
		if !bytes.Equal(v, d.Data) {
			return e.New("not equal %v", string(v))
		}
		return nil
	})
	if err != nil {
		t.Fatal(e.Trace(e.Forward(err)))
	}
}
