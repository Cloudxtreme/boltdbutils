// Copyright 2015 Felipe A. Cavani. All rights reserved.
// Use of this source code is governed by the Apache License 2.0
// license that can be found in the LICENSE file.

package boltdbutils

import (
	"bytes"
	"io/ioutil"
	"path/filepath"
	"testing"

	"github.com/boltdb/bolt"
	"github.com/fcavani/e"
	"github.com/fcavani/rand"
)

func skiprev(n uint64, c *Cursor, data []testData) error {
	keys, val := c.Skip(n)
	if err := c.Err(); err != nil {
		return e.Forward(err)
	}
	if keys == nil {
		return e.New("skip returned nil")
	}
	n = uint64(len(data)) - n - 1
	if !bytes.Equal(val, data[n].Data) {
		return e.New("not equal %v", string(val))
	}
	for i, key := range keys {
		if !bytes.Equal(key, data[n].Keys[i]) {
			return e.New("key is not equal %v %v", string(key), string(data[n].Keys[i]))
		}
	}
	return nil
}

func TestCursorRevSkip(t *testing.T) {
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
			Reverse: true,
		}
		c.Init()

		var i uint64
		for ; i < uint64(len(data))-1; i++ {
			err := skiprev(i, c, data)
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

func TestCursorRevSeek(t *testing.T) {
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
			Reverse: true,
		}
		c.Init()
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
		{[]byte("test_bucket"), [][]byte{[]byte("key1"), []byte("key4")}, []byte("13")},
		{[]byte("test_bucket"), [][]byte{[]byte("key2"), []byte("key3")}, []byte("22")},
		{[]byte("test_bucket"), [][]byte{[]byte("key4"), []byte("key1")}, nil},
	}

	err = db.View(func(tx *bolt.Tx) error {
		c := &Cursor{
			Tx:      tx,
			Bucket:  []byte("test_bucket"),
			NumKeys: 2,
			Reverse: true,
		}
		c.Init()
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

func TestCursorRevFirstLast(t *testing.T) {
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
			Reverse: true,
		}
		c.Init()

		keys, val := c.First()
		if keys == nil {
			return e.New("First returned nil")
		}
		l := len(data) - 1
		if !bytes.Equal(val, data[l].Data) {
			return e.New("not equal %v %v", string(val), string(data[l].Data))
		}
		for i, key := range keys {
			if !bytes.Equal(key, data[l].Keys[i]) {
				return e.New("key is not equal %v %v", string(key), string(data[l].Keys[i]))
			}
		}

		keys, val = c.Last()
		if keys == nil {
			return e.New("First returned nil")
		}
		l = 0
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

func TestCursorRevNext(t *testing.T) {
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
			Reverse: true,
		}
		c.Init()
		i := len(data) - 1
		for k, v := c.First(); k != nil; k, v = c.Next() {
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
		return nil
	})
	if err != nil {
		t.Fatal(e.Trace(e.Forward(err)))
	}
}

func TestCursorRevPrev(t *testing.T) {
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
			Reverse: true,
		}
		c.Init()
		i := 0
		for k, v := c.Last(); k != nil; k, v = c.Prev() {
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

func TestCursorRevSkipKeys(t *testing.T) {
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
			Reverse: true,
		}
		err := c.Init([]byte("key2"))
		if err != nil {
			return e.Forward(err)
		}

		//Next
		i := 4
		for k, v := c.First(); k != nil; k, v = c.Next() {
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
		if i > 2 {
			t.Fatal("iterator didn't stop", i)
		}

		//Prev
		i = 3
		for k, v := c.Last(); k != nil; k, v = c.Prev() {
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
		if i < 5 {
			t.Fatal("iterator didn't stop", i)
		}

		//Skip
		var m uint64
		var j int = 4
		for ; j < 2; j-- {
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
		// k, v := c.Seek(data[0].Keys...)
		// if k != nil {
		// 	return e.New("must be nil")
		// }

		k, v := c.Seek([]byte("bú"), []byte("key3"))
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
