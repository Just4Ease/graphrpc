// Copyright 2012-2021 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"io/ioutil"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nkeys"
)

func require_True(t *testing.T, b bool) {
	t.Helper()
	if !b {
		t.Fatalf("require true, but got false")
	}
}

func require_False(t *testing.T, b bool) {
	t.Helper()
	if b {
		t.Fatalf("require no false, but got true")
	}
}

func require_NoError(t testing.TB, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("require no error, but got: %v", err)
	}
}

func require_Contains(t *testing.T, s string, subStrs ...string) {
	t.Helper()
	for _, subStr := range subStrs {
		if !strings.Contains(s, subStr) {
			t.Fatalf("require %q to be contained in %q", subStr, s)
		}
	}
}

func require_Error(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatalf("require error, but got none")
	}
}

func require_Equal(t *testing.T, a, b string) {
	t.Helper()
	if strings.Compare(a, b) != 0 {
		t.Fatalf("require equal, but got: %v != %v", a, b)
	}
}

func require_NotEqual(t *testing.T, a, b [32]byte) {
	t.Helper()
	if bytes.Equal(a[:], b[:]) {
		t.Fatalf("require not equal, but got: %v != %v", a, b)
	}
}

func require_Len(t *testing.T, a, b int) {
	t.Helper()
	if a != b {
		t.Fatalf("require len, but got: %v != %v", a, b)
	}
}

func TestShardedDirStoreWriteAndReadonly(t *testing.T) {
	t.Parallel()
	dir := createDir(t, "jwtstore_test")

	store, err := NewDirJWTStore(dir, true, false)
	require_NoError(t, err)

	expected := map[string]string{
		"one":   "alpha",
		"two":   "beta",
		"three": "gamma",
		"four":  "delta",
	}

	for k, v := range expected {
		store.SaveAcc(k, v)
	}

	for k, v := range expected {
		got, err := store.LoadAcc(k)
		require_NoError(t, err)
		require_Equal(t, v, got)
	}

	got, err := store.LoadAcc("random")
	require_Error(t, err)
	require_Equal(t, "", got)

	got, err = store.LoadAcc("")
	require_Error(t, err)
	require_Equal(t, "", got)

	err = store.SaveAcc("", "onetwothree")
	require_Error(t, err)
	store.Close()

	// re-use the folder for readonly mode
	store, err = NewImmutableDirJWTStore(dir, true)
	require_NoError(t, err)

	require_True(t, store.IsReadOnly())

	err = store.SaveAcc("five", "omega")
	require_Error(t, err)

	for k, v := range expected {
		got, err := store.LoadAcc(k)
		require_NoError(t, err)
		require_Equal(t, v, got)
	}
	store.Close()
}

func TestUnshardedDirStoreWriteAndReadonly(t *testing.T) {
	t.Parallel()
	dir := createDir(t, "jwtstore_test")

	store, err := NewDirJWTStore(dir, false, false)
	require_NoError(t, err)

	expected := map[string]string{
		"one":   "alpha",
		"two":   "beta",
		"three": "gamma",
		"four":  "delta",
	}

	require_False(t, store.IsReadOnly())

	for k, v := range expected {
		store.SaveAcc(k, v)
	}

	for k, v := range expected {
		got, err := store.LoadAcc(k)
		require_NoError(t, err)
		require_Equal(t, v, got)
	}

	got, err := store.LoadAcc("random")
	require_Error(t, err)
	require_Equal(t, "", got)

	got, err = store.LoadAcc("")
	require_Error(t, err)
	require_Equal(t, "", got)

	err = store.SaveAcc("", "onetwothree")
	require_Error(t, err)
	store.Close()

	// re-use the folder for readonly mode
	store, err = NewImmutableDirJWTStore(dir, false)
	require_NoError(t, err)

	require_True(t, store.IsReadOnly())

	err = store.SaveAcc("five", "omega")
	require_Error(t, err)

	for k, v := range expected {
		got, err := store.LoadAcc(k)
		require_NoError(t, err)
		require_Equal(t, v, got)
	}
	store.Close()
}

func TestNoCreateRequiresDir(t *testing.T) {
	t.Parallel()
	_, err := NewDirJWTStore("/a/b/c", true, false)
	require_Error(t, err)
}

func TestCreateMakesDir(t *testing.T) {
	t.Parallel()
	dir := createDir(t, "jwtstore_test")

	fullPath := filepath.Join(dir, "a/b")

	_, err := os.Stat(fullPath)
	require_Error(t, err)
	require_True(t, os.IsNotExist(err))

	s, err := NewDirJWTStore(fullPath, false, true)
	require_NoError(t, err)
	s.Close()

	_, err = os.Stat(fullPath)
	require_NoError(t, err)
}

func TestShardedDirStorePackMerge(t *testing.T) {
	t.Parallel()
	dir := createDir(t, "jwtstore_test")
	dir2 := createDir(t, "jwtstore_test")
	dir3 := createDir(t, "jwtstore_test")

	store, err := NewDirJWTStore(dir, true, false)
	require_NoError(t, err)

	expected := map[string]string{
		"one":   "alpha",
		"two":   "beta",
		"three": "gamma",
		"four":  "delta",
	}

	require_False(t, store.IsReadOnly())

	for k, v := range expected {
		store.SaveAcc(k, v)
	}

	for k, v := range expected {
		got, err := store.LoadAcc(k)
		require_NoError(t, err)
		require_Equal(t, v, got)
	}

	got, err := store.LoadAcc("random")
	require_Error(t, err)
	require_Equal(t, "", got)

	pack, err := store.Pack(-1)
	require_NoError(t, err)

	inc, err := NewDirJWTStore(dir2, true, false)
	require_NoError(t, err)

	inc.Merge(pack)

	for k, v := range expected {
		got, err := inc.LoadAcc(k)
		require_NoError(t, err)
		require_Equal(t, v, got)
	}

	got, err = inc.LoadAcc("random")
	require_Error(t, err)
	require_Equal(t, "", got)

	limitedPack, err := inc.Pack(1)
	require_NoError(t, err)

	limited, err := NewDirJWTStore(dir3, true, false)

	require_NoError(t, err)

	limited.Merge(limitedPack)

	count := 0
	for k, v := range expected {
		got, err := limited.LoadAcc(k)
		if err == nil {
			count++
			require_Equal(t, v, got)
		}
	}

	require_Len(t, 1, count)

	got, err = inc.LoadAcc("random")
	require_Error(t, err)
	require_Equal(t, "", got)
}

func TestShardedToUnsharedDirStorePackMerge(t *testing.T) {
	t.Parallel()
	dir := createDir(t, "jwtstore_test")
	dir2 := createDir(t, "jwtstore_test")

	store, err := NewDirJWTStore(dir, true, false)
	require_NoError(t, err)

	expected := map[string]string{
		"one":   "alpha",
		"two":   "beta",
		"three": "gamma",
		"four":  "delta",
	}

	require_False(t, store.IsReadOnly())

	for k, v := range expected {
		store.SaveAcc(k, v)
	}

	for k, v := range expected {
		got, err := store.LoadAcc(k)
		require_NoError(t, err)
		require_Equal(t, v, got)
	}

	got, err := store.LoadAcc("random")
	require_Error(t, err)
	require_Equal(t, "", got)

	pack, err := store.Pack(-1)
	require_NoError(t, err)

	inc, err := NewDirJWTStore(dir2, false, false)
	require_NoError(t, err)

	inc.Merge(pack)

	for k, v := range expected {
		got, err := inc.LoadAcc(k)
		require_NoError(t, err)
		require_Equal(t, v, got)
	}

	got, err = inc.LoadAcc("random")
	require_Error(t, err)
	require_Equal(t, "", got)

	err = store.Merge("foo")
	require_Error(t, err)

	err = store.Merge("") // will skip it
	require_NoError(t, err)

	err = store.Merge("a|something") // should fail on a for sharding
	require_Error(t, err)
}

func TestMergeOnlyOnNewer(t *testing.T) {
	t.Parallel()
	dir := createDir(t, "jwtstore_test")

	dirStore, err := NewDirJWTStore(dir, true, false)
	require_NoError(t, err)

	accountKey, err := nkeys.CreateAccount()
	require_NoError(t, err)

	pubKey, err := accountKey.PublicKey()
	require_NoError(t, err)

	account := jwt.NewAccountClaims(pubKey)
	account.Name = "old"
	olderJWT, err := account.Encode(accountKey)
	require_NoError(t, err)

	time.Sleep(2 * time.Second)

	account.Name = "new"
	newerJWT, err := account.Encode(accountKey)
	require_NoError(t, err)

	// Should work
	err = dirStore.SaveAcc(pubKey, olderJWT)
	require_NoError(t, err)
	fromStore, err := dirStore.LoadAcc(pubKey)
	require_NoError(t, err)
	require_Equal(t, olderJWT, fromStore)

	// should replace
	err = dirStore.saveIfNewer(pubKey, newerJWT)
	require_NoError(t, err)
	fromStore, err = dirStore.LoadAcc(pubKey)
	require_NoError(t, err)
	require_Equal(t, newerJWT, fromStore)

	// should fail
	err = dirStore.saveIfNewer(pubKey, olderJWT)
	require_NoError(t, err)
	fromStore, err = dirStore.LoadAcc(pubKey)
	require_NoError(t, err)
	require_Equal(t, newerJWT, fromStore)
}

func createTestAccount(t *testing.T, dirStore *DirJWTStore, expSec int, accKey nkeys.KeyPair) string {
	t.Helper()
	pubKey, err := accKey.PublicKey()
	require_NoError(t, err)
	account := jwt.NewAccountClaims(pubKey)
	if expSec > 0 {
		account.Expires = time.Now().Round(time.Second).Add(time.Second * time.Duration(expSec)).Unix()
	}
	jwt, err := account.Encode(accKey)
	require_NoError(t, err)
	err = dirStore.SaveAcc(pubKey, jwt)
	require_NoError(t, err)
	return jwt
}

func assertStoreSize(t *testing.T, dirStore *DirJWTStore, length int) {
	t.Helper()
	f, err := ioutil.ReadDir(dirStore.directory)
	require_NoError(t, err)
	require_Len(t, len(f), length)
	dirStore.Lock()
	require_Len(t, len(dirStore.expiration.idx), length)
	require_Len(t, dirStore.expiration.lru.Len(), length)
	require_Len(t, len(dirStore.expiration.heap), length)
	dirStore.Unlock()
}

func TestExpiration(t *testing.T) {
	t.Parallel()
	dir := createDir(t, "jwtstore_test")

	dirStore, err := NewExpiringDirJWTStore(dir, false, false, NoDelete, time.Millisecond*50, 10, true, 0, nil)
	require_NoError(t, err)
	defer dirStore.Close()

	account := func(expSec int) {
		accountKey, err := nkeys.CreateAccount()
		require_NoError(t, err)
		createTestAccount(t, dirStore, expSec, accountKey)
	}

	hBegin := dirStore.Hash()
	account(100)
	hNoExp := dirStore.Hash()
	require_NotEqual(t, hBegin, hNoExp)
	account(1)
	nh2 := dirStore.Hash()
	require_NotEqual(t, hNoExp, nh2)
	assertStoreSize(t, dirStore, 2)

	failAt := time.Now().Add(4 * time.Second)
	for time.Now().Before(failAt) {
		time.Sleep(100 * time.Millisecond)
		f, err := ioutil.ReadDir(dir)
		require_NoError(t, err)
		if len(f) == 1 {
			lh := dirStore.Hash()
			require_Equal(t, string(hNoExp[:]), string(lh[:]))
			return
		}
	}
	t.Fatalf("Waited more than 4 seconds for the file with expiration 1 second to expire")
}

func TestLimit(t *testing.T) {
	t.Parallel()
	dir := createDir(t, "jwtstore_test")

	dirStore, err := NewExpiringDirJWTStore(dir, false, false, NoDelete, time.Millisecond*100, 5, true, 0, nil)
	require_NoError(t, err)
	defer dirStore.Close()

	account := func(expSec int) {
		accountKey, err := nkeys.CreateAccount()
		require_NoError(t, err)
		createTestAccount(t, dirStore, expSec, accountKey)
	}

	h := dirStore.Hash()

	accountKey, err := nkeys.CreateAccount()
	require_NoError(t, err)
	// update first account
	for i := 0; i < 10; i++ {
		createTestAccount(t, dirStore, 50, accountKey)
		assertStoreSize(t, dirStore, 1)
	}
	// new accounts
	for i := 0; i < 10; i++ {
		account(i)
		nh := dirStore.Hash()
		require_NotEqual(t, h, nh)
		h = nh
	}
	// first account should be gone now accountKey.PublicKey()
	key, _ := accountKey.PublicKey()
	_, err = os.Stat(fmt.Sprintf("%s/%s.jwt", dir, key))
	require_True(t, os.IsNotExist(err))

	// update first account
	for i := 0; i < 10; i++ {
		createTestAccount(t, dirStore, 50, accountKey)
		assertStoreSize(t, dirStore, 5)
	}
}

func TestLimitNoEvict(t *testing.T) {
	t.Parallel()
	dir := createDir(t, "jwtstore_test")

	dirStore, err := NewExpiringDirJWTStore(dir, false, false, NoDelete, time.Millisecond*50, 2, false, 0, nil)
	require_NoError(t, err)
	defer dirStore.Close()

	accountKey1, err := nkeys.CreateAccount()
	require_NoError(t, err)
	pKey1, err := accountKey1.PublicKey()
	require_NoError(t, err)
	accountKey2, err := nkeys.CreateAccount()
	require_NoError(t, err)
	accountKey3, err := nkeys.CreateAccount()
	require_NoError(t, err)
	pKey3, err := accountKey3.PublicKey()
	require_NoError(t, err)

	createTestAccount(t, dirStore, 100, accountKey1)
	assertStoreSize(t, dirStore, 1)
	createTestAccount(t, dirStore, 1, accountKey2)
	assertStoreSize(t, dirStore, 2)

	hBefore := dirStore.Hash()
	// 2 jwt are already stored. third must result in an error
	pubKey, err := accountKey3.PublicKey()
	require_NoError(t, err)
	account := jwt.NewAccountClaims(pubKey)
	jwt, err := account.Encode(accountKey3)
	require_NoError(t, err)
	err = dirStore.SaveAcc(pubKey, jwt)
	require_Error(t, err)
	assertStoreSize(t, dirStore, 2)
	_, err = os.Stat(fmt.Sprintf("%s/%s.jwt", dir, pKey1))
	require_NoError(t, err)
	_, err = os.Stat(fmt.Sprintf("%s/%s.jwt", dir, pKey3))
	require_True(t, os.IsNotExist(err))
	// check that the hash did not change
	hAfter := dirStore.Hash()
	require_True(t, bytes.Equal(hBefore[:], hAfter[:]))
	// wait for expiration of account2
	time.Sleep(2200 * time.Millisecond)
	err = dirStore.SaveAcc(pubKey, jwt)
	require_NoError(t, err)
	assertStoreSize(t, dirStore, 2)
	_, err = os.Stat(fmt.Sprintf("%s/%s.jwt", dir, pKey1))
	require_NoError(t, err)
	_, err = os.Stat(fmt.Sprintf("%s/%s.jwt", dir, pKey3))
	require_NoError(t, err)
}

func TestLruLoad(t *testing.T) {
	t.Parallel()
	dir := createDir(t, "jwtstore_test")
	dirStore, err := NewExpiringDirJWTStore(dir, false, false, NoDelete, time.Millisecond*100, 2, true, 0, nil)
	require_NoError(t, err)
	defer dirStore.Close()

	accountKey1, err := nkeys.CreateAccount()
	require_NoError(t, err)
	pKey1, err := accountKey1.PublicKey()
	require_NoError(t, err)
	accountKey2, err := nkeys.CreateAccount()
	require_NoError(t, err)
	accountKey3, err := nkeys.CreateAccount()
	require_NoError(t, err)
	pKey3, err := accountKey3.PublicKey()
	require_NoError(t, err)

	createTestAccount(t, dirStore, 10, accountKey1)
	assertStoreSize(t, dirStore, 1)
	createTestAccount(t, dirStore, 10, accountKey2)
	assertStoreSize(t, dirStore, 2)
	dirStore.LoadAcc(pKey1) // will reorder 1/2
	createTestAccount(t, dirStore, 10, accountKey3)
	assertStoreSize(t, dirStore, 2)

	_, err = os.Stat(fmt.Sprintf("%s/%s.jwt", dir, pKey1))
	require_NoError(t, err)
	_, err = os.Stat(fmt.Sprintf("%s/%s.jwt", dir, pKey3))
	require_NoError(t, err)
}

func TestLruVolume(t *testing.T) {
	t.Parallel()
	dir := createDir(t, "jwtstore_test")

	dirStore, err := NewExpiringDirJWTStore(dir, false, false, NoDelete, time.Millisecond*50, 2, true, 0, nil)
	require_NoError(t, err)
	defer dirStore.Close()
	replaceCnt := 500 // needs to be bigger than 2 due to loop unrolling
	keys := make([]string, replaceCnt)

	key, err := nkeys.CreateAccount()
	require_NoError(t, err)
	keys[0], err = key.PublicKey()
	require_NoError(t, err)
	createTestAccount(t, dirStore, 10000, key) // not intended to expire
	assertStoreSize(t, dirStore, 1)

	key, err = nkeys.CreateAccount()
	require_NoError(t, err)
	keys[1], err = key.PublicKey()
	require_NoError(t, err)
	createTestAccount(t, dirStore, 10000, key)
	assertStoreSize(t, dirStore, 2)

	for i := 2; i < replaceCnt; i++ {
		k, err := nkeys.CreateAccount()
		require_NoError(t, err)
		keys[i], err = k.PublicKey()
		require_NoError(t, err)

		createTestAccount(t, dirStore, 10000+rand.Intn(10000), k) // not intended to expire
		assertStoreSize(t, dirStore, 2)
		_, err = os.Stat(fmt.Sprintf("%s/%s.jwt", dir, keys[i-2]))
		require_Error(t, err)
		require_True(t, os.IsNotExist(err))
		_, err = os.Stat(fmt.Sprintf("%s/%s.jwt", dir, keys[i-1]))
		require_NoError(t, err)
		_, err = os.Stat(fmt.Sprintf("%s/%s.jwt", dir, keys[i]))
		require_NoError(t, err)
	}
}

func TestLru(t *testing.T) {
	t.Parallel()
	dir := createDir(t, "jwtstore_test")

	dirStore, err := NewExpiringDirJWTStore(dir, false, false, NoDelete, time.Millisecond*50, 2, true, 0, nil)
	require_NoError(t, err)
	defer dirStore.Close()

	accountKey1, err := nkeys.CreateAccount()
	require_NoError(t, err)
	pKey1, err := accountKey1.PublicKey()
	require_NoError(t, err)
	accountKey2, err := nkeys.CreateAccount()
	require_NoError(t, err)
	accountKey3, err := nkeys.CreateAccount()
	require_NoError(t, err)
	pKey3, err := accountKey3.PublicKey()
	require_NoError(t, err)

	createTestAccount(t, dirStore, 1000, accountKey1)
	assertStoreSize(t, dirStore, 1)
	createTestAccount(t, dirStore, 1000, accountKey2)
	assertStoreSize(t, dirStore, 2)
	createTestAccount(t, dirStore, 1000, accountKey3)
	assertStoreSize(t, dirStore, 2)
	_, err = os.Stat(fmt.Sprintf("%s/%s.jwt", dir, pKey1))
	require_Error(t, err)
	require_True(t, os.IsNotExist(err))
	_, err = os.Stat(fmt.Sprintf("%s/%s.jwt", dir, pKey3))
	require_NoError(t, err)

	// update -> will change this keys position for eviction
	createTestAccount(t, dirStore, 1000, accountKey2)
	assertStoreSize(t, dirStore, 2)
	// recreate -> will evict 3
	createTestAccount(t, dirStore, 1, accountKey1)
	assertStoreSize(t, dirStore, 2)
	_, err = os.Stat(fmt.Sprintf("%s/%s.jwt", dir, pKey3))
	require_True(t, os.IsNotExist(err))
	// let key1 expire. sleep expSec=1 + 1 for rounding
	time.Sleep(2200 * time.Millisecond)
	assertStoreSize(t, dirStore, 1)
	_, err = os.Stat(fmt.Sprintf("%s/%s.jwt", dir, pKey1))
	require_True(t, os.IsNotExist(err))
	// recreate key3 - no eviction
	createTestAccount(t, dirStore, 1000, accountKey3)
	assertStoreSize(t, dirStore, 2)
}

func TestReload(t *testing.T) {
	t.Parallel()
	dir := createDir(t, "jwtstore_test")
	notificationChan := make(chan struct{}, 5)
	dirStore, err := NewExpiringDirJWTStore(dir, false, false, NoDelete, time.Millisecond*100, 2, true, 0, func(publicKey string) {
		notificationChan <- struct{}{}
	})
	require_NoError(t, err)
	defer dirStore.Close()
	newAccount := func() string {
		t.Helper()
		accKey, err := nkeys.CreateAccount()
		require_NoError(t, err)
		pKey, err := accKey.PublicKey()
		require_NoError(t, err)
		pubKey, err := accKey.PublicKey()
		require_NoError(t, err)
		account := jwt.NewAccountClaims(pubKey)
		jwt, err := account.Encode(accKey)
		require_NoError(t, err)
		file := fmt.Sprintf("%s/%s.jwt", dir, pKey)
		err = ioutil.WriteFile(file, []byte(jwt), 0644)
		require_NoError(t, err)
		return file
	}
	files := make(map[string]struct{})
	assertStoreSize(t, dirStore, 0)
	hash := dirStore.Hash()
	emptyHash := [sha256.Size]byte{}
	require_True(t, bytes.Equal(hash[:], emptyHash[:]))
	for i := 0; i < 5; i++ {
		files[newAccount()] = struct{}{}
		err = dirStore.Reload()
		require_NoError(t, err)
		<-notificationChan
		assertStoreSize(t, dirStore, i+1)
		hash = dirStore.Hash()
		require_False(t, bytes.Equal(hash[:], emptyHash[:]))
		msg, err := dirStore.Pack(-1)
		require_NoError(t, err)
		require_Len(t, len(strings.Split(msg, "\n")), len(files))
	}
	for k := range files {
		hash = dirStore.Hash()
		require_False(t, bytes.Equal(hash[:], emptyHash[:]))
		removeFile(t, k)
		err = dirStore.Reload()
		require_NoError(t, err)
		assertStoreSize(t, dirStore, len(files)-1)
		delete(files, k)
		msg, err := dirStore.Pack(-1)
		require_NoError(t, err)
		if len(files) != 0 { // when len is 0, we have an empty line
			require_Len(t, len(strings.Split(msg, "\n")), len(files))
		}
	}
	require_True(t, len(notificationChan) == 0)
	hash = dirStore.Hash()
	require_True(t, bytes.Equal(hash[:], emptyHash[:]))
}

func TestExpirationUpdate(t *testing.T) {
	t.Parallel()
	dir := createDir(t, "jwtstore_test")

	dirStore, err := NewExpiringDirJWTStore(dir, false, false, NoDelete, time.Millisecond*50, 10, true, 0, nil)
	require_NoError(t, err)
	defer dirStore.Close()

	accountKey, err := nkeys.CreateAccount()
	require_NoError(t, err)

	h := dirStore.Hash()

	createTestAccount(t, dirStore, 0, accountKey)
	nh := dirStore.Hash()
	require_NotEqual(t, h, nh)
	h = nh

	time.Sleep(1500 * time.Millisecond)
	f, err := ioutil.ReadDir(dir)
	require_NoError(t, err)
	require_Len(t, len(f), 1)

	createTestAccount(t, dirStore, 2, accountKey)
	nh = dirStore.Hash()
	require_NotEqual(t, h, nh)
	h = nh

	time.Sleep(1500 * time.Millisecond)
	f, err = ioutil.ReadDir(dir)
	require_NoError(t, err)
	require_Len(t, len(f), 1)

	createTestAccount(t, dirStore, 0, accountKey)
	nh = dirStore.Hash()
	require_NotEqual(t, h, nh)
	h = nh

	time.Sleep(1500 * time.Millisecond)
	f, err = ioutil.ReadDir(dir)
	require_NoError(t, err)
	require_Len(t, len(f), 1)

	createTestAccount(t, dirStore, 1, accountKey)
	nh = dirStore.Hash()
	require_NotEqual(t, h, nh)
	h = nh

	time.Sleep(1500 * time.Millisecond)
	f, err = ioutil.ReadDir(dir)
	require_NoError(t, err)
	require_Len(t, len(f), 0)

	empty := [32]byte{}
	h = dirStore.Hash()
	require_Equal(t, string(h[:]), string(empty[:]))
}

func TestTTL(t *testing.T) {
	t.Parallel()
	dir := createDir(t, "jwtstore_test")
	require_OneJWT := func() {
		t.Helper()
		f, err := ioutil.ReadDir(dir)
		require_NoError(t, err)
		require_Len(t, len(f), 1)
	}
	test := func(op func(store *DirJWTStore, accountKey nkeys.KeyPair, accountPubKey string, jwt string)) {
		dirStore, err := NewExpiringDirJWTStore(dir, false, false, NoDelete, 50*time.Millisecond, 10, true, 200*time.Millisecond, nil)
		require_NoError(t, err)
		defer dirStore.Close()

		accountKey, err := nkeys.CreateAccount()
		require_NoError(t, err)
		pubKey, err := accountKey.PublicKey()
		require_NoError(t, err)
		jwt := createTestAccount(t, dirStore, 0, accountKey)
		require_OneJWT()
		// observe non expiration due to activity
		for i := 0; i < 4; i++ {
			time.Sleep(110 * time.Millisecond)
			op(dirStore, accountKey, pubKey, jwt)
			require_OneJWT()
		}
		// observe expiration
		for i := 0; i < 40; i++ {
			time.Sleep(50 * time.Millisecond)
			f, err := ioutil.ReadDir(dir)
			require_NoError(t, err)
			if len(f) == 0 {
				return
			}
		}
		t.Fatalf("jwt should have expired by now")
	}
	t.Run("no expiration due to load", func(t *testing.T) {
		test(func(store *DirJWTStore, accountKey nkeys.KeyPair, pubKey string, jwt string) {
			store.LoadAcc(pubKey)
		})
	})
	t.Run("no expiration due to store", func(t *testing.T) {
		test(func(store *DirJWTStore, accountKey nkeys.KeyPair, pubKey string, jwt string) {
			store.SaveAcc(pubKey, jwt)
		})
	})
	t.Run("no expiration due to overwrite", func(t *testing.T) {
		test(func(store *DirJWTStore, accountKey nkeys.KeyPair, pubKey string, jwt string) {
			createTestAccount(t, store, 0, accountKey)
		})
	})
}

func TestRemove(t *testing.T) {
	for deleteType, test := range map[deleteType]struct {
		expected int
		moved    int
	}{
		HardDelete:    {0, 0},
		RenameDeleted: {0, 1},
		NoDelete:      {1, 0},
	} {
		t.Run("", func(t *testing.T) {
			t.Parallel()
			dir := createDir(t, "jwtstore_test")
			require_OneJWT := func() {
				t.Helper()
				f, err := ioutil.ReadDir(dir)
				require_NoError(t, err)
				require_Len(t, len(f), 1)
			}
			dirStore, err := NewExpiringDirJWTStore(dir, false, false, deleteType, 0, 10, true, 0, nil)
			delPubKey := ""
			dirStore.deleted = func(publicKey string) {
				delPubKey = publicKey
			}
			require_NoError(t, err)
			defer dirStore.Close()
			accountKey, err := nkeys.CreateAccount()
			require_NoError(t, err)
			pubKey, err := accountKey.PublicKey()
			require_NoError(t, err)
			createTestAccount(t, dirStore, 0, accountKey)
			require_OneJWT()
			dirStore.delete(pubKey)
			if deleteType == NoDelete {
				require_True(t, delPubKey == "")
			} else {
				require_True(t, delPubKey == pubKey)
			}
			f, err := filepath.Glob(dir + string(os.PathSeparator) + "/*.jwt")
			require_NoError(t, err)
			require_Len(t, len(f), test.expected)
			f, err = filepath.Glob(dir + string(os.PathSeparator) + "/*.jwt.deleted")
			require_NoError(t, err)
			require_Len(t, len(f), test.moved)
		})
	}
}

const infDur = time.Duration(math.MaxInt64)

func TestNotificationOnPack(t *testing.T) {
	t.Parallel()
	jwts := map[string]string{
		"key1": "value",
		"key2": "value",
		"key3": "value",
		"key4": "value",
	}
	notificationChan := make(chan struct{}, len(jwts)) // set to same len so all extra will block
	notification := func(pubKey string) {
		if _, ok := jwts[pubKey]; !ok {
			t.Fatalf("Key not found: %s", pubKey)
		}
		notificationChan <- struct{}{}
	}
	dirPack := createDir(t, "jwtstore_test")
	packStore, err := NewExpiringDirJWTStore(dirPack, false, false, NoDelete, infDur, 0, true, 0, notification)
	require_NoError(t, err)
	// prefill the store with data
	for k, v := range jwts {
		require_NoError(t, packStore.SaveAcc(k, v))
	}
	for i := 0; i < len(jwts); i++ {
		<-notificationChan
	}
	msg, err := packStore.Pack(-1)
	require_NoError(t, err)
	packStore.Close()
	hash := packStore.Hash()
	for _, shard := range []bool{true, false, true, false} {
		dirMerge := createDir(t, "jwtstore_test")
		mergeStore, err := NewExpiringDirJWTStore(dirMerge, shard, false, NoDelete, infDur, 0, true, 0, notification)
		require_NoError(t, err)
		// set
		err = mergeStore.Merge(msg)
		require_NoError(t, err)
		assertStoreSize(t, mergeStore, len(jwts))
		hash1 := packStore.Hash()
		require_True(t, bytes.Equal(hash[:], hash1[:]))
		for i := 0; i < len(jwts); i++ {
			<-notificationChan
		}
		// overwrite - assure
		err = mergeStore.Merge(msg)
		require_NoError(t, err)
		assertStoreSize(t, mergeStore, len(jwts))
		hash2 := packStore.Hash()
		require_True(t, bytes.Equal(hash1[:], hash2[:]))

		hash = hash1
		msg, err = mergeStore.Pack(-1)
		require_NoError(t, err)
		mergeStore.Close()
		require_True(t, len(notificationChan) == 0)

		for k, v := range jwts {
			j, err := packStore.LoadAcc(k)
			require_NoError(t, err)
			require_Equal(t, j, v)
		}
	}
}

func TestNotificationOnPackWalk(t *testing.T) {
	t.Parallel()
	const storeCnt = 5
	const keyCnt = 50
	const iterCnt = 8
	store := [storeCnt]*DirJWTStore{}
	for i := 0; i < storeCnt; i++ {
		dirMerge := createDir(t, "jwtstore_test")
		mergeStore, err := NewExpiringDirJWTStore(dirMerge, true, false, NoDelete, infDur, 0, true, 0, nil)
		require_NoError(t, err)
		store[i] = mergeStore
	}
	for i := 0; i < iterCnt; i++ { //iterations
		jwt := make(map[string]string)
		for j := 0; j < keyCnt; j++ {
			key := fmt.Sprintf("key%d-%d", i, j)
			value := "value"
			jwt[key] = value
			store[0].SaveAcc(key, value)
		}
		for j := 0; j < storeCnt-1; j++ { // stores
			err := store[j].PackWalk(3, func(partialPackMsg string) {
				err := store[j+1].Merge(partialPackMsg)
				require_NoError(t, err)
			})
			require_NoError(t, err)
		}
		for i := 0; i < storeCnt-1; i++ {
			h1 := store[i].Hash()
			h2 := store[i+1].Hash()
			require_True(t, bytes.Equal(h1[:], h2[:]))
		}
	}
	for i := 0; i < storeCnt; i++ {
		store[i].Close()
	}
}
