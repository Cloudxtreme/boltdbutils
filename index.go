// Copyright 2015 Felipe A. Cavani. All rights reserved.
// Use of this source code is governed by the Apache License 2.0
// license that can be found in the LICENSE file.

package boltdbutils

import (
	"github.com/boltdb/bolt"
	"github.com/fcavani/e"
	"github.com/fcavani/rand"
)

// pub -> year -> month -> day -> text
// year -> month -> day -> text
//
// NewDateCursor(reverse bool, onlypub bool)
//
// Seek(year, month, day int, title string)
//
// title -> Text
// code -> Text

func Put(tx *bolt.Tx, bucket []byte, keys [][]byte, data []byte) error {
	var err error
	var buf []byte
	var b *bolt.Bucket
	b, err = tx.CreateBucketIfNotExists(bucket)
	if err != nil {
		return e.Forward(err)
	}
	if len(keys) == 0 {
		return e.New("no keys")
	}
	if len(keys) >= 2 {
		for i := 0; i < len(keys)-1; i++ {
			buf = b.Get(keys[i])
			if buf == nil {
				id, err := rand.Uuid()
				if err != nil {
					return e.Forward(err)
				}
				buf = []byte(id)
				err = b.Put(keys[i], buf)
				if err != nil {
					return e.Forward(err)
				}
			}
			b, err = tx.CreateBucket(buf)
			if e.Contains(err, "bucket already exists") {
				b = tx.Bucket(buf)
			} else if err != nil {
				return e.Forward(err)
			}
		}
	}
	err = b.Put(keys[len(keys)-1], data)
	if err != nil {
		return e.Forward(err)
	}
	return nil
}

const ErrKeyNotFound = "key not found"

func Get(tx *bolt.Tx, bucket []byte, keys [][]byte) ([]byte, error) {
	var buf []byte
	if len(keys) == 0 {
		return nil, e.New("no keys")
	}
	b := tx.Bucket(bucket)
	if len(keys) >= 2 {
		for _, key := range keys[:len(keys)-1] {
			buf = b.Get(key)
			if buf == nil {
				return nil, e.New(ErrKeyNotFound)
			}
			b = tx.Bucket(buf)
		}
	}
	buf = b.Get(keys[len(keys)-1])
	if buf == nil {
		return nil, e.New(ErrKeyNotFound)
	}
	return buf, nil
}

func Del(tx *bolt.Tx, bucket []byte, keys [][]byte) error {
	if len(keys) == 0 {
		return e.New("no keys")
	}
	bname := make([][]byte, len(keys))
	bs := make([]*bolt.Bucket, len(keys))
	b := tx.Bucket(bucket)
	bname[0] = bucket
	bs[0] = b
	for i := 0; i < len(keys); i++ {
		v := b.Get(keys[i])
		b = tx.Bucket(v)
		if i+1 < len(keys) {
			bname[i+1] = v
			bs[i+1] = b
		}
	}

	for level := len(bs) - 1; level >= 0; level-- {
		err := bs[level].Delete(keys[level])
		if err != nil {
			return e.Forward(err)
		}
		if bs[level].Stats().KeyN <= 1 {
			if level-1 < 0 {
				break
			}
			err = tx.DeleteBucket(bname[level])
			if err != nil {
				return e.Forward(err)
			}
			continue
		}
		break
	}
	return nil
}
