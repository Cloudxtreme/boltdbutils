// Copyright 2015 Felipe A. Cavani. All rights reserved.
// Use of this source code is governed by the Apache License 2.0
// license that can be found in the LICENSE file.

package boltdbutils

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"log"
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

func TestDel(t *testing.T) {
	buckets := []string{"test_del"}
	data := []testData{
		{[]byte("test_del"), [][]byte{[]byte("key-a1"), []byte("key-b1"), []byte("key-c1")}, []byte("epson")},
		{[]byte("test_del"), [][]byte{[]byte("key-a2"), []byte("key-b2"), []byte("key-c2")}, []byte("catoto")},
		{[]byte("test_del"), [][]byte{[]byte("key-a3"), []byte("key-b3"), []byte("key-c3")}, []byte("catoto")},
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

	err = PrintDb(db, buckets)
	if err != nil {
		t.Fatal(e.Trace(e.Forward(err)))
	}

	err = db.Update(func(tx *bolt.Tx) error {
		for i, d := range data {
			err := Del(tx, d.Bucket, d.Keys)
			if err != nil {
				PrintDbTx(tx, buckets)
				return e.Push(err, e.New("Fail to del %v", i))
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(e.Trace(e.Forward(err)))
	}
	err = PrintDb(db, buckets)
	if err != nil {
		t.Fatal(e.Trace(e.Forward(err)))
	}
	err = DbEmpty(db, buckets)
	if err != nil {
		t.Fatal(e.Trace(e.Forward(err)))
	}
}

func DbEmpty(db *bolt.DB, buckets []string) error {
	err := db.View(func(tx *bolt.Tx) error {
		err := tx.ForEach(func(name []byte, b *bolt.Bucket) error {
			found := false
			for _, bucket := range buckets {
				if bucket == string(name) {
					found = true
					break
				}
			}
			if found {
				return nil
			}
			return e.New("found a bucket named %v", string(name))
		})
		return e.Forward(err)
	})
	return e.Forward(err)
}

func PrintDb(db *bolt.DB, buckets []string) error {
	err := db.View(func(tx *bolt.Tx) error {
		// err := tx.ForEach(func(name []byte, b *bolt.Bucket) error {
		// 	fmt.Println(string(name))
		// 	return nil
		// })
		for _, bucket := range buckets {
			b := tx.Bucket([]byte(bucket))
			if b == nil {
				log.Printf("Bucket %v not found.", bucket)
				continue
			}
			fmt.Println(bucket, b.Stats().KeyN)
			err := b.ForEach(func(k, v []byte) error {
				fmt.Printf("\t%v -> %v\n", string(k), string(v))
				err := goInside(tx, v, 2)
				if err != nil {
					return e.Forward(err)
				}
				return nil
			})
			if err != nil {
				return e.Forward(err)
			}
		}
		return nil
	})
	if err != nil {
		return e.Forward(err)
	}
	return nil
}

func PrintDbTx(tx *bolt.Tx, buckets []string) error {
	for _, bucket := range buckets {
		b := tx.Bucket([]byte(bucket))
		if b == nil {
			log.Printf("Bucket %v not found.", bucket)
			continue
		}
		fmt.Println(bucket, b.Stats().KeyN)
		err := b.ForEach(func(k, v []byte) error {
			fmt.Printf("\t%v -> %v\n", string(k), string(v))
			err := goInside(tx, v, 2)
			if err != nil {
				return e.Forward(err)
			}
			return nil
		})
		if err != nil {
			return e.Forward(err)
		}
	}
	return nil
}

func goInside(tx *bolt.Tx, v []byte, level int) error {
	sub := tx.Bucket(v)
	if sub == nil {
		return e.New("bucket %v not found", string(v))
	}
	//fmt.Println(string(v), sub.Stats().KeyN)
	err := sub.ForEach(func(k, v []byte) error {
		for i := 0; i < level; i++ {
			fmt.Print("\t")
		}
		fmt.Printf("%v (%v) -> %v\n", string(k), decNumber(k), string(v))
		err := goInside(tx, v, level+1)
		if err != nil {
			return nil
		}
		return nil
	})
	if err != nil {
		return e.Forward(err)
	}
	return nil
}

func decNumber(buf []byte) int64 {
	num, _ := binary.Varint(buf)
	return num
}
