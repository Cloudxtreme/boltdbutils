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

type testData struct {
	Bucket []byte
	Keys   [][]byte
	Data   []byte
}

func TestIndex(t *testing.T) {
	data := []testData{
		{[]byte("test_bucket1"), [][]byte{[]byte("key1")}, []byte("lorem")},
		{[]byte("test_bucket2"), [][]byte{[]byte("key1"), []byte("key2")}, []byte("datadatadatadatadata")},
		{[]byte("test_bucket2"), [][]byte{[]byte("key1"), []byte("key3")}, []byte("3")},
		{[]byte("test_bucket3"), [][]byte{[]byte("key1"), []byte("key2"), []byte("key3")}, []byte("catoto")},
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
		for i, d := range data {
			data, err := Get(tx, d.Bucket, d.Keys)
			if err != nil {
				return e.Push(err, e.New("Fail to get %v", i))
			}
			if !bytes.Equal(data, d.Data) {
				return e.New("not equal %v", i)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(e.Trace(e.Forward(err)))
	}

	err = db.Update(func(tx *bolt.Tx) error {
		err = Del(tx, data[0].Bucket, data[0].Keys)
		if err != nil {
			return e.Push(err, e.New("Fail to del %v", 0))
		}
		return nil
	})
	if err != nil {
		t.Fatal(e.Trace(e.Forward(err)))
	}

	err = db.View(func(tx *bolt.Tx) error {
		for i, d := range data {
			data, err := Get(tx, d.Bucket, d.Keys)
			if i == 0 {
				if err != nil && !e.Equal(err, ErrKeyNotFound) {
					return e.Push(err, "fail with the wrong error")
				} else if err == nil {
					return e.New("not fail")
				}
				continue
			}
			if err != nil {
				return e.Push(err, e.New("Fail to get %v", i))
			}
			if !bytes.Equal(data, d.Data) {
				return e.New("not equal %v", i)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(e.Trace(e.Forward(err)))
	}

	err = db.Update(func(tx *bolt.Tx) error {
		for i, d := range data[1:] {
			err := Del(tx, d.Bucket, d.Keys)
			if err != nil {
				return e.Push(err, e.New("Fail to del %v", i))
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(e.Trace(e.Forward(err)))
	}

	err = db.View(func(tx *bolt.Tx) error {
		for i, d := range data[1:] {
			_, err := Get(tx, d.Bucket, d.Keys)
			if err != nil && !e.Equal(err, ErrKeyNotFound) {
				return e.Push(err, e.New("Fail to get %v", i))
			} else if err == nil {
				return e.New("nil error")
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(e.Trace(e.Forward(err)))
	}
}
