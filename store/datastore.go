package store

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"mrcroxx.io/hermes/log"
	"mrcroxx.io/hermes/pkg"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
)

var (
	errOutOfRange = errors.New("index out of range")
	ext           = ".blk"
)

type DataStore interface {
	Get(n uint64) ([]string, uint64)        // get data <- * fresh data
	Append(vs []string) uint64              // append data -> fresh data *
	Cache(n uint64) uint64                  // cache data <- * fresh data
	Uncache(index uint64)                   // cache data -> fresh data
	Persist(n uint64) (uint64, error)       // persist data <- * cache data
	DeleteCache(n uint64)                   // delete data <- * cache data
	CleanBlockFiles(nremain uint64)         // delete block file
	ReadStorage(index uint64) <-chan string // persist data -> memory
	GetSnapshot() ([]byte, error)
	RecoverFromSnapshot([]byte) error
	Indexes() (deletedIndex uint64, persistedIndex uint64, cachedIndex uint64, freshIndex uint64)
}

type dataStore struct {
	DeletedIndex   uint64
	PersistedIndex uint64
	CachedIndex    uint64
	FreshIndex     uint64

	StorageDir string

	CachedData []string
	FreshData  []string
}

func NewDataStore(storageDir string) DataStore {
	ds := &dataStore{
		DeletedIndex:   0,
		PersistedIndex: 0,
		CachedIndex:    0,
		FreshIndex:     0,
		CachedData:     make([]string, 0),
		FreshData:      make([]string, 0),
		StorageDir:     storageDir,
	}
	return ds
}

func (ds *dataStore) Get(n uint64) ([]string, uint64) {
	nfresh := uint64(len(ds.FreshData))
	if nfresh < n {
		n = nfresh
	}
	result := ds.FreshData[:n]
	return result, n
}

func (ds *dataStore) Append(vs []string) uint64 {
	ds.FreshData = append(ds.FreshData, vs...)
	ds.FreshIndex += uint64(len(vs))
	return uint64(len(vs))
}

func (ds *dataStore) Cache(n uint64) uint64 {
	if uint64(len(ds.FreshData)) < n {
		n = uint64(len(ds.FreshData))
	}
	ds.CachedData = append(ds.CachedData, ds.FreshData[:n]...)
	ds.FreshData = ds.FreshData[n:]
	ds.CachedIndex += n
	return n
}

func (ds *dataStore) Uncache(index uint64) {
	if index > ds.CachedIndex {
		return
	}
	offset := index - (ds.PersistedIndex + 1)
	log.ZAPSugaredLogger().Debugf("Uncache %d data", len(ds.CachedData[offset:]))
	ds.FreshData = append(ds.CachedData[offset:], ds.FreshData...)
	ds.CachedData = ds.CachedData[:offset]
	ds.CachedIndex = index - 1
}

func (ds *dataStore) Persist(n uint64) (uint64, error) {
	if uint64(len(ds.CachedData)) < n {
		return 0, errOutOfRange
	}
	data, err := pkg.Encode(ds.CachedData[:n])
	if err != nil {
		return 0, err
	}
	err = pkg.Write(path.Join(ds.StorageDir, ds.encodeBlockFileName(ds.PersistedIndex, int(n))), data)
	if err != nil {
		return 0, err
	}
	return n, nil
}

func (ds *dataStore) DeleteCache(n uint64) {
	ds.CachedData = ds.CachedData[n:]
	ds.PersistedIndex += n
}

func (ds *dataStore) ReadStorage(index uint64) <-chan string {
	c := make(chan string)
	go ds.loadStorage(index, c)
	return c
}

func (ds *dataStore) loadStorage(index uint64, c chan<- string) {
	fs := ds.listBlockFiles()
	type STF struct {
		s uint64
		t uint64
		f string
	}
	stfs := []STF{}
	for _, f := range fs {
		s, t, b := ds.decodeBlockFileName(f)
		if !b {
			continue
		}
		stfs = append(stfs, STF{
			s: s,
			t: t,
			f: f,
		})
	}
	sort.Slice(stfs, func(i, j int) bool {
		if stfs[i].s == stfs[j].s {
			return stfs[i].t > stfs[j].t
		}
		return stfs[i].s < stfs[j].s
	})
	now := index
	for _, stf := range stfs {
		if stf.t <= now {
			continue
		}
		f, err := os.Open(stf.f)
		if err != nil {
			continue
		}
		r := bufio.NewReader(f)
		offset := uint64(0)
		if now > stf.s {
			offset = now - stf.s
		}
		fi := uint64(0)
		for {
			l, _, err := r.ReadLine()
			if err == io.EOF {
				break
			}
			fi++
			if fi > offset {
				c <- string(l)
			}
		}
		now = stf.t
		f.Close()
	}
	close(c)
}

func (ds *dataStore) Indexes() (deletedIndex uint64, persistedIndex uint64, cachedIndex uint64, freshIndex uint64) {
	return ds.DeletedIndex, ds.PersistedIndex, ds.CachedIndex, ds.FreshIndex
}

func (ds *dataStore) CleanBlockFiles(nremain uint64) {
	fs := ds.listBlockFiles()
	if ds.PersistedIndex <= nremain {
		return
	}
	deleteIndex := ds.PersistedIndex - nremain

	firstRemainIndex := deleteIndex
	for _, f := range fs {
		s, t, b := ds.decodeBlockFileName(f)
		if !b {
			continue
		}
		if t < deleteIndex {
			if err := os.Remove(f); err != nil {
				continue
			}
		} else {
			if firstRemainIndex > s {
				firstRemainIndex = s
			}
		}
	}
	if firstRemainIndex > 0 {
		ds.DeletedIndex = firstRemainIndex - 1
	}
}

func (ds *dataStore) listBlockFiles() []string {
	p := ds.StorageDir
	dirInfo, err := os.Stat(p)
	if err != nil || !dirInfo.IsDir() {
		return []string{}
	}
	infos, err := ioutil.ReadDir(p)
	if err != nil {
		return []string{}
	}
	r := []string{}
	for _, info := range infos {
		if path.Ext(info.Name()) == ext {
			r = append(r, path.Join(p, info.Name()))
		}
	}
	return r
}

func (ds *dataStore) encodeBlockFileName(s uint64, n int) string {
	return fmt.Sprintf("%016x-%016x%s", s, s+uint64(n), ext)
}

func (ds *dataStore) decodeBlockFileName(p string) (uint64, uint64, bool) {
	//log.ZAPSugaredLogger().Debugf("decoding .blk file : %s", p)
	info, err := os.Stat(p)
	if err != nil || info.IsDir() || path.Ext(p) != ext {
		return 0, 0, false
	}
	ss := strings.Split(info.Name(), ".")
	if len(ss) != 2 {
		return 0, 0, false
	}
	ids := strings.Split(ss[0], "-")
	if len(ids) != 2 {
		return 0, 0, false
	}
	x, err := strconv.ParseUint(ids[0], 16, 64)
	if err != nil {
		return 0, 0, false
	}
	y, err := strconv.ParseUint(ids[1], 16, 64)
	if err != nil {
		return 0, 0, false
	}
	return x, y, true
}

func (ds *dataStore) GetSnapshot() ([]byte, error) {
	return pkg.Encode(ds)
}

func (ds *dataStore) RecoverFromSnapshot(snap []byte) error {
	return pkg.Decode(snap, ds)
}
