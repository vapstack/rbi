package rbi

import (
	"fmt"
	"math/rand"
	"reflect"
	"slices"
	"sync"
	"testing"

	"github.com/vapstack/qx"
)

func TestIOExt_ConcurrentUint64ReadersWriters_NoPanicsOrDecodeErrors(t *testing.T) {
	c, _ := openTempUint64Collection(t)
	c.disableSync()
	defer c.enableSync()

	const keys = 8
	for i := 1; i <= keys; i++ {
		ioExtMustSetRec(t, c, uint64(i), &Rec{
			Name: fmt.Sprintf("seed-%d", i),
			Age:  i,
			Tags: []string{fmt.Sprintf("t%d", i%3)},
			Meta: Meta{Country: "NL"},
		})
	}

	ids := make([]uint64, keys)
	for i := range ids {
		ids[i] = uint64(i + 1)
	}

	errCh := make(chan error, 128)
	var writers sync.WaitGroup
	var readers sync.WaitGroup

	for w := 0; w < 2; w++ {
		w := w
		writers.Add(1)
		go func() {
			defer writers.Done()
			defer func() {
				if r := recover(); r != nil {
					errCh <- fmt.Errorf("writer panic: %v", r)
				}
			}()
			for i := 0; i < 150; i++ {
				id := uint64((i+w)%keys + 1)
				switch i % 3 {
				case 0:
					if err := writeSet(c, id, &Rec{
						Name: fmt.Sprintf("w%d-set-%d", w, i),
						Age:  i,
						Tags: []string{fmt.Sprintf("set-%d", i%5)},
						Meta: Meta{Country: "PL"},
					}); err != nil {
						errCh <- fmt.Errorf("writer=%d Set(%d): %w", w, id, err)
						return
					}
				case 1:
					if err := writePatch(c, id, []Field{
						{Name: "age", Value: 1000 + i},
						{Name: "country", Value: "DE"},
					}); err != nil {
						errCh <- fmt.Errorf("writer=%d Patch(%d): %w", w, id, err)
						return
					}
				default:
					if err := writeDelete(c, id); err != nil {
						errCh <- fmt.Errorf("writer=%d Delete(%d): %w", w, id, err)
						return
					}
				}
			}
		}()
	}

	for r := 0; r < 3; r++ {
		r := r
		readers.Add(1)
		go func() {
			defer readers.Done()
			defer func() {
				if rr := recover(); rr != nil {
					errCh <- fmt.Errorf("reader panic: %v", rr)
				}
			}()
			for i := 0; i < 120; i++ {
				id := uint64((i+r)%keys + 1)
				v, err := readGet(c, id)
				if err != nil {
					errCh <- fmt.Errorf("reader=%d Get(%d): %w", r, id, err)
					return
				}
				if v != nil && v.Name == "" {
					errCh <- fmt.Errorf("reader=%d Get(%d): empty name", r, id)
					return
				}

				vals, err := readValues(c, ids...)
				if err != nil {
					errCh <- fmt.Errorf("reader=%d readValues: %w", r, err)
					return
				}
				for i, v := range vals {
					if v != nil && v.Name == "" {
						errCh <- fmt.Errorf("reader=%d readValues[%d]: empty name", r, i)
						return
					}
				}

				if err := readSeqScan(c, 0, func(id uint64, v *Rec) (bool, error) {
					if v.Name == "" {
						return false, fmt.Errorf("SeqScan id=%d empty name", id)
					}
					return true, nil
				}); err != nil {
					errCh <- fmt.Errorf("reader=%d SeqScan: %w", r, err)
					return
				}

				if err := scanRawBolt(t, c, 0, func(id uint64, raw []byte) (bool, error) {
					cp := append([]byte(nil), raw...)
					v, err := c.decode(cp)
					if err != nil {
						return false, fmt.Errorf("decode raw id=%d: %w", id, err)
					}
					defer c.ReleaseRecords(v)
					if v.Name == "" {
						return false, fmt.Errorf("raw id=%d empty name", id)
					}
					return true, nil
				}); err != nil {
					errCh <- fmt.Errorf("reader=%d raw bbolt scan: %w", r, err)
					return
				}

				if err := readScanKeys(c, 0, func(id uint64) (bool, error) {
					if id == 0 {
						return false, fmt.Errorf("zero key")
					}
					return true, nil
				}); err != nil {
					errCh <- fmt.Errorf("reader=%d ScanKeys: %w", r, err)
					return
				}
			}
		}()
	}

	writers.Wait()
	readers.Wait()
	close(errCh)

	for err := range errCh {
		t.Error(err)
	}
	if t.Failed() {
		t.FailNow()
	}
	if _, err := readValues(c, ids...); err != nil {
		t.Fatalf("final readValues: %v", err)
	}
}

func TestIOExt_ConcurrentStringReadersWriters_NoPanicsOrDecodeErrors(t *testing.T) {
	c, _ := openTempCollectionStringProduct(t)
	c.disableSync()
	defer c.enableSync()

	keys := []string{"k1", "k2", "k3", "k4", "k5", "k6"}
	for i, key := range keys {
		ioExtMustSetProduct(t, c, key, &Product{
			SKU:   key,
			Price: float64(i + 1),
			Tags:  []string{fmt.Sprintf("t%d", i%3)},
		})
	}

	errCh := make(chan error, 128)
	var writers sync.WaitGroup
	var readers sync.WaitGroup

	for w := 0; w < 2; w++ {
		w := w
		writers.Add(1)
		go func() {
			defer writers.Done()
			defer func() {
				if r := recover(); r != nil {
					errCh <- fmt.Errorf("writer panic: %v", r)
				}
			}()
			for i := 0; i < 150; i++ {
				key := keys[(i+w)%len(keys)]
				switch i % 3 {
				case 0:
					if err := writeSet(c, key, &Product{
						SKU:   key,
						Price: float64(i + 1),
						Tags:  []string{fmt.Sprintf("set-%d", i%5)},
					}); err != nil {
						errCh <- fmt.Errorf("writer=%d Set(%q): %w", w, key, err)
						return
					}
				case 1:
					if err := writePatch(c, key, []Field{
						{Name: "price", Value: float64(1000 + i)},
						{Name: "tags", Value: []string{fmt.Sprintf("patch-%d", i%5)}},
					}); err != nil {
						errCh <- fmt.Errorf("writer=%d Patch(%q): %w", w, key, err)
						return
					}
				default:
					if err := writeDelete(c, key); err != nil {
						errCh <- fmt.Errorf("writer=%d Delete(%q): %w", w, key, err)
						return
					}
				}
			}
		}()
	}

	for r := 0; r < 3; r++ {
		r := r
		readers.Add(1)
		go func() {
			defer readers.Done()
			defer func() {
				if rr := recover(); rr != nil {
					errCh <- fmt.Errorf("reader panic: %v", rr)
				}
			}()
			for i := 0; i < 120; i++ {
				key := keys[(i+r)%len(keys)]
				v, err := readGet(c, key)
				if err != nil {
					errCh <- fmt.Errorf("reader=%d Get(%q): %w", r, key, err)
					return
				}
				if v != nil && v.SKU == "" {
					errCh <- fmt.Errorf("reader=%d Get(%q): empty sku", r, key)
					return
				}

				vals, err := readValues(c, keys...)
				if err != nil {
					errCh <- fmt.Errorf("reader=%d readValues: %w", r, err)
					return
				}
				for i, v := range vals {
					if v != nil && v.SKU == "" {
						errCh <- fmt.Errorf("reader=%d readValues[%d]: empty sku", r, i)
						return
					}
				}

				if err := readSeqScan(c, "", func(id string, v *Product) (bool, error) {
					if id == "" || v.SKU == "" {
						return false, fmt.Errorf("SeqScan empty id/sku")
					}
					return true, nil
				}); err != nil {
					errCh <- fmt.Errorf("reader=%d SeqScan: %w", r, err)
					return
				}

				if err := scanRawBolt(t, c, "", func(id string, raw []byte) (bool, error) {
					payload, err := rawPayloadForTest(c, raw)
					if err != nil {
						return false, fmt.Errorf("parse raw id=%q: %w", id, err)
					}
					cp := append([]byte(nil), payload...)
					v, err := c.decode(cp)
					if err != nil {
						return false, fmt.Errorf("decode raw id=%q: %w", id, err)
					}
					defer c.ReleaseRecords(v)
					if id == "" || v.SKU == "" {
						return false, fmt.Errorf("raw empty id/sku")
					}
					return true, nil
				}); err != nil {
					errCh <- fmt.Errorf("reader=%d raw bbolt scan: %w", r, err)
					return
				}

				ids, err := readQueryKeys(c, qx.Query())
				if err != nil {
					errCh <- fmt.Errorf("reader=%d QueryKeys(all): %w", r, err)
					return
				}
				for _, id := range ids {
					if id == "" {
						errCh <- fmt.Errorf("reader=%d empty key", r)
						return
					}
				}
			}
		}()
	}

	writers.Wait()
	readers.Wait()
	close(errCh)

	for err := range errCh {
		t.Error(err)
	}
	if t.Failed() {
		t.FailNow()
	}

	scanned := ioExtCollectSeqScanProduct(t, c, "")
	for key, v := range scanned {
		if key == "" || v.SKU == "" {
			t.Fatalf("invalid final scanned product: key=%q value=%#v", key, v)
		}
		raw := ioExtMustReadStringRaw(t, c, key)
		payload, err := rawPayloadForTest(c, raw)
		if err != nil {
			t.Fatalf("final raw payload(%q): %v", key, err)
		}
		decoded, err := c.decode(payload)
		if err != nil {
			t.Fatalf("final decode(%q): %v", key, err)
		}
		if decoded.SKU == "" {
			c.ReleaseRecords(decoded)
			t.Fatalf("final decoded product has empty sku for key=%q", key)
		}
		c.ReleaseRecords(decoded)
	}
}

func TestConcurrentWriters_FinalStateAndIndexConsistency(t *testing.T) {
	c, _ := openTempUint64Collection(t)

	const (
		writers    = 8
		opsPerW    = 320
		idSpace    = 96
		readers    = 4
		staleProbe = 256
	)

	var historicNames sync.Map

	errCh := make(chan error, writers+readers+16)
	stopReaders := make(chan struct{})

	readQueries := []*qx.QX{
		qx.Query(qx.PREFIX("name", "id-")),
		qx.Query(qx.GTE("age", 0)),
		qx.Query(qx.HASANY("tags", []string{"w0", "w1", "w2", "w3"})),
		qx.Query(qx.EQ("active", true)),
	}

	var readersWG sync.WaitGroup
	for r := 0; r < readers; r++ {
		readersWG.Add(1)
		go func(readerID int) {
			defer readersWG.Done()
			i := 0
			for {
				select {
				case <-stopReaders:
					return
				default:
				}
				q := readQueries[(readerID+i)%len(readQueries)]
				i++
				if _, err := readQueryKeys(c, q); err != nil {
					errCh <- fmt.Errorf("reader QueryKeys error: %w", err)
					return
				}
				if _, err := readQuery(c, q); err != nil {
					errCh <- fmt.Errorf("reader Query error: %w", err)
					return
				}
				if _, err := readCount(c, q.Filter); err != nil {
					errCh <- fmt.Errorf("reader Count error: %w", err)
					return
				}
			}
		}(r)
	}

	var writersWG sync.WaitGroup
	for w := 0; w < writers; w++ {
		w := w
		writersWG.Add(1)
		go func() {
			defer writersWG.Done()
			for i := 0; i < opsPerW; i++ {
				id := uint64((w*131+i*17)%idSpace + 1)
				if (w+i)%5 == 0 {
					if err := writeDelete(c, id); err != nil {
						errCh <- fmt.Errorf("writer=%d Delete(%d): %w", w, id, err)
						return
					}
					continue
				}

				name := fmt.Sprintf("id-%03d-w%02d-op%04d", id, w, i)
				age := w*10_000 + i
				historicNames.Store(name, struct{}{})
				rec := &Rec{
					Name:   name,
					Age:    age,
					Active: i%2 == 0,
					Tags: []string{
						fmt.Sprintf("w%d", w%4),
						fmt.Sprintf("slot-%d", i%7),
					},
					Meta: Meta{Country: "NL"},
				}
				if err := writeSet(c, id, rec); err != nil {
					errCh <- fmt.Errorf("writer=%d Set(%d): %w", w, id, err)
					return
				}
			}
		}()
	}

	writersWG.Wait()
	close(stopReaders)
	readersWG.Wait()
	close(errCh)

	for err := range errCh {
		t.Errorf("%v", err)
	}
	if t.Failed() {
		t.FailNow()
	}

	type liveRec struct {
		id   uint64
		name string
		age  int
	}
	live := make(map[uint64]liveRec, idSpace)
	err := readSeqScan(c, 0, func(id uint64, rec *Rec) (bool, error) {
		if rec == nil {
			return false, fmt.Errorf("nil record for id=%d", id)
		}
		if rec.Name == "" {
			return false, fmt.Errorf("empty name for id=%d", id)
		}
		live[id] = liveRec{id: id, name: rec.Name, age: rec.Age}
		return true, nil
	})
	if err != nil {
		t.Fatalf("SeqScan: %v", err)
	}

	total, err := readCount(c)
	if err != nil {
		t.Fatalf("Count(): %v", err)
	}
	if total != uint64(len(live)) {
		t.Fatalf("count mismatch: got=%d want=%d", total, len(live))
	}

	liveNames := make(map[string]uint64, len(live))
	for id, rec := range live {
		ids, err := readQueryKeys(c, qx.Query(qx.EQ("name", rec.name)))
		if err != nil {
			t.Fatalf("QueryKeys(name=%q): %v", rec.name, err)
		}
		if len(ids) != 1 || ids[0] != id {
			t.Fatalf("name index mismatch for %q: got=%v want=[%d]", rec.name, ids, id)
		}
		liveNames[rec.name] = id
	}

	checked := 0
	historicNames.Range(func(k, _ any) bool {
		if checked >= staleProbe {
			return false
		}
		name, ok := k.(string)
		if !ok {
			return true
		}
		if _, stillLive := liveNames[name]; stillLive {
			return true
		}
		ids, err := readQueryKeys(c, qx.Query(qx.EQ("name", name)))
		if err != nil {
			t.Fatalf("QueryKeys(stale name=%q): %v", name, err)
		}
		if len(ids) != 0 {
			t.Fatalf("stale name %q still indexed: ids=%v", name, ids)
		}
		checked++
		return true
	})
}

func TestConcurrentBatchWriters_ModelReplayConsistency(t *testing.T) {
	c, _ := openTempUint64Collection(t)

	const (
		writers    = 6
		opsPerW    = 220
		idSpace    = 128
		readers    = 3
		staleProbe = 320
	)

	makeBatchIDs := func(writer, iter, size int) []uint64 {
		out := make([]uint64, 0, size)
		used := make(map[uint64]struct{}, size)
		base := writer*1009 + iter*67 + 11
		for len(out) < size {
			cand := uint64((base+len(out)*17+writer*13+iter*7)%idSpace + 1)
			if _, ok := used[cand]; ok {
				base++
				continue
			}
			used[cand] = struct{}{}
			out = append(out, cand)
		}
		return out
	}

	var historicNames sync.Map

	errCh := make(chan error, writers+readers+16)
	stopReaders := make(chan struct{})

	readQueries := []*qx.QX{
		qx.Query(qx.PREFIX("name", "bm-id")),
		qx.Query(qx.GTE("age", 0)),
		qx.Query(qx.EQ("active", true)),
		qx.Query(qx.HASANY("tags", []string{"bw0", "bw1", "bw2", "bw3"})),
	}

	var readersWG sync.WaitGroup
	for r := 0; r < readers; r++ {
		readersWG.Add(1)
		go func(readerID int) {
			defer readersWG.Done()
			i := 0
			for {
				select {
				case <-stopReaders:
					return
				default:
				}
				q := readQueries[(readerID+i)%len(readQueries)]
				i++
				if _, err := readQueryKeys(c, q); err != nil {
					errCh <- fmt.Errorf("reader QueryKeys error: %w", err)
					return
				}
				if _, err := readQuery(c, q); err != nil {
					errCh <- fmt.Errorf("reader Query error: %w", err)
					return
				}
				if _, err := readCount(c, q.Filter); err != nil {
					errCh <- fmt.Errorf("reader Count error: %w", err)
					return
				}
			}
		}(r)
	}

	var writersWG sync.WaitGroup
	for w := 0; w < writers; w++ {
		w := w
		writersWG.Add(1)
		go func() {
			defer writersWG.Done()
			for i := 0; i < opsPerW; i++ {
				opType := (w*37 + i*13) % 3
				switch opType {
				case 0: // MultiSet
					size := 2 + ((w + i) % 4)
					ids := makeBatchIDs(w, i, size)
					vals := make([]*Rec, 0, len(ids))
					for p, id := range ids {
						name := fmt.Sprintf("bm-id%03d-w%02d-i%04d-p%02d", id, w, i, p)
						age := w*100_000 + i*10 + p
						active := (i+p)%2 == 0
						historicNames.Store(name, struct{}{})
						vals = append(vals, &Rec{
							Name:   name,
							Age:    age,
							Active: active,
							Tags: []string{
								fmt.Sprintf("bw%d", w%4),
								fmt.Sprintf("grp-%d", i%9),
							},
							Meta: Meta{Country: "NL"},
						})
					}
					if err := writeSets(c, ids, vals); err != nil {
						errCh <- fmt.Errorf("writer=%d MultiSet: %w", w, err)
						return
					}

				case 1: // MultiPatch
					size := 2 + ((w + i + 1) % 4)
					ids := makeBatchIDs(w+19, i+7, size)
					patchAge := 900_000 + w*1000 + i
					patchActive := i%2 == 0
					patch := []Field{
						{Name: "age", Value: patchAge},
						{Name: "active", Value: patchActive},
					}
					if err := writePatches(c, ids, patch); err != nil {
						errCh <- fmt.Errorf("writer=%d MultiPatch: %w", w, err)
						return
					}

				default: // MultiDelete
					size := 1 + ((w + i + 2) % 4)
					ids := makeBatchIDs(w+41, i+3, size)
					if err := writeDeletes(c, ids); err != nil {
						errCh <- fmt.Errorf("writer=%d MultiDelete: %w", w, err)
						return
					}
				}
			}
		}()
	}

	writersWG.Wait()
	close(stopReaders)
	readersWG.Wait()
	close(errCh)

	for err := range errCh {
		t.Errorf("%v", err)
	}
	if t.Failed() {
		t.FailNow()
	}

	type liveRec struct {
		name   string
		age    int
		active bool
	}
	live := make(map[uint64]liveRec, idSpace)
	err := readSeqScan(c, 0, func(id uint64, rec *Rec) (bool, error) {
		if rec == nil {
			return false, fmt.Errorf("nil record for id=%d", id)
		}
		live[id] = liveRec{name: rec.Name, age: rec.Age, active: rec.Active}
		return true, nil
	})
	if err != nil {
		t.Fatalf("SeqScan: %v", err)
	}

	total, err := readCount(c)
	if err != nil {
		t.Fatalf("Count(): %v", err)
	}
	if total != uint64(len(live)) {
		t.Fatalf("count mismatch: got=%d want=%d", total, len(live))
	}

	liveNames := make(map[string]uint64, len(live))
	for id, exp := range live {
		v, err := readGet(c, id)
		if err != nil {
			t.Fatalf("Get(%d): %v", id, err)
		}
		if v == nil {
			t.Fatalf("Get(%d): expected non-nil value", id)
		}
		if v.Name != exp.name || v.Age != exp.age || v.Active != exp.active {
			t.Fatalf(
				"record mismatch for id=%d: got={name:%q age:%d active:%v} want={name:%q age:%d active:%v}",
				id, v.Name, v.Age, v.Active, exp.name, exp.age, exp.active,
			)
		}

		ids, err := readQueryKeys(c, qx.Query(qx.EQ("name", exp.name)))
		if err != nil {
			t.Fatalf("QueryKeys(name=%q): %v", exp.name, err)
		}
		if len(ids) != 1 || ids[0] != id {
			t.Fatalf("name index mismatch for %q: got=%v want=[%d]", exp.name, ids, id)
		}
		liveNames[exp.name] = id
	}

	checked := 0
	historicNames.Range(func(k, _ any) bool {
		if checked >= staleProbe {
			return false
		}
		name, ok := k.(string)
		if !ok {
			return true
		}
		if _, stillLive := liveNames[name]; stillLive {
			return true
		}
		ids, err := readQueryKeys(c, qx.Query(qx.EQ("name", name)))
		if err != nil {
			t.Fatalf("QueryKeys(stale name=%q): %v", name, err)
		}
		if len(ids) != 0 {
			t.Fatalf("stale name %q still indexed: ids=%v", name, ids)
		}
		checked++
		return true
	})
}

func TestBatchConcurrentSingleOps_ModelReplayConsistency(t *testing.T) {
	enableStoreStatsForTest(t)

	c, _ := openTempUint64Collection(t, Options{
		BatchSoftLimit: 32,
	})

	const (
		writers    = 8
		opsPerW    = 220
		idSpace    = 96
		staleProbe = 512
	)

	type op struct {
		kind  uint8 // 0=set, 1=patch_if_exists, 2=delete
		id    uint64
		rec   *Rec
		patch []Field
	}

	cloneRec := func(v *Rec) *Rec {
		if v == nil {
			return nil
		}
		cp := *v
		cp.Tags = slices.Clone(v.Tags)
		if v.Opt != nil {
			s := *v.Opt
			cp.Opt = &s
		}
		return &cp
	}
	clonePatch := func(in []Field) []Field {
		out := make([]Field, len(in))
		for i := range in {
			out[i] = in[i]
			if tags, ok := in[i].Value.([]string); ok {
				out[i].Value = slices.Clone(tags)
			}
		}
		return out
	}
	var (
		logMu sync.Mutex
		logs  = make([]op, 0, writers*opsPerW)
	)

	errCh := make(chan error, writers)
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		w := w
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := rand.New(rand.NewSource(7300 + int64(w)*101))
			countries := []string{"NL", "PL", "DE", "FI", "IS", "TH", "US"}
			names := []string{"alice", "bob", "carol", "dave", "eve", "nik"}
			tags := []string{"go", "db", "ops", "rust", "java", "infra", "ml"}

			randTags := func() []string {
				n := 1 + r.Intn(4)
				out := make([]string, 0, n)
				for i := 0; i < n; i++ {
					out = append(out, tags[r.Intn(len(tags))])
				}
				return out
			}

			for i := 0; i < opsPerW; i++ {
				id := uint64(1 + r.Intn(idSpace))
				switch r.Intn(3) {
				case 0: // Set
					rec := &Rec{
						Meta:     Meta{Country: countries[r.Intn(len(countries))]},
						Name:     names[r.Intn(len(names))],
						Email:    fmt.Sprintf("w%02d-id%03d-i%04d@example.test", w, id, i),
						Age:      18 + r.Intn(62),
						Score:    float64(r.Intn(5000)) / 10.0,
						Active:   r.Intn(2) == 0,
						Tags:     randTags(),
						FullName: fmt.Sprintf("ID-%03d", id),
					}
					if err := writeSet(c, id, rec); err != nil {
						errCh <- fmt.Errorf("writer=%d set id=%d: %w", w, id, err)
						return
					}
					logMu.Lock()
					logs = append(logs, op{
						kind: 0,
						id:   id,
						rec:  cloneRec(rec),
					})
					logMu.Unlock()

				case 1: // Patch
					var patch []Field
					switch r.Intn(6) {
					case 0:
						patch = []Field{{Name: "name", Value: names[r.Intn(len(names))]}}
					case 1:
						patch = []Field{{Name: "age", Value: 18 + r.Intn(62)}}
					case 2:
						patch = []Field{{Name: "country", Value: countries[r.Intn(len(countries))]}}
					case 3:
						patch = []Field{{Name: "active", Value: r.Intn(2) == 0}}
					case 4:
						patch = []Field{{Name: "score", Value: float64(r.Intn(5000)) / 10.0}}
					default:
						patch = []Field{{Name: "tags", Value: randTags()}}
					}
					if err := writePatch(c, id, patch); err != nil {
						errCh <- fmt.Errorf("writer=%d patch id=%d patch=%v: %w", w, id, patch, err)
						return
					}
					logMu.Lock()
					logs = append(logs, op{
						kind:  1,
						id:    id,
						patch: clonePatch(patch),
					})
					logMu.Unlock()

				default: // Delete
					if err := writeDelete(c, id); err != nil {
						errCh <- fmt.Errorf("writer=%d delete id=%d: %w", w, id, err)
						return
					}
					logMu.Lock()
					logs = append(logs, op{
						kind: 2,
						id:   id,
					})
					logMu.Unlock()
				}
			}
		}()
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Errorf("%v", err)
	}
	if t.Failed() {
		t.FailNow()
	}

	waitAutoBatchExtraStats(t, c.root, "concurrent multi-unit accounting settled", func(st rootSchedulerSnapshot) bool {
		return st.MultiUnitBatches != 0
	})
	bs := c.StoreStats()
	if bs.RejectedClosed != 0 {
		t.Fatalf("unexpected root scheduler rejected stats: %+v", bs)
	}
	if bs.MultiUnitBatches == 0 {
		t.Fatalf("expected at least one multi-request batch, stats=%+v", bs)
	}

	logMu.Lock()
	ops := slices.Clone(logs)
	logMu.Unlock()

	type singleOpHistory struct {
		names     map[string]struct{}
		emails    map[string]struct{}
		countries map[string]struct{}
		ages      map[int]struct{}
		scores    map[float64]struct{}
		actives   map[bool]struct{}
		tags      map[string]struct{}
	}
	getHistory := func(m map[uint64]*singleOpHistory, id uint64) *singleOpHistory {
		h := m[id]
		if h != nil {
			return h
		}
		h = &singleOpHistory{
			names:     make(map[string]struct{}),
			emails:    make(map[string]struct{}),
			countries: make(map[string]struct{}),
			ages:      make(map[int]struct{}),
			scores:    make(map[float64]struct{}),
			actives:   make(map[bool]struct{}),
			tags:      make(map[string]struct{}),
		}
		m[id] = h
		return h
	}

	historyByID := make(map[uint64]*singleOpHistory, idSpace)
	for _, o := range ops {
		h := getHistory(historyByID, o.id)
		switch o.kind {
		case 0:
			if o.rec == nil {
				continue
			}
			h.names[o.rec.Name] = struct{}{}
			h.emails[o.rec.Email] = struct{}{}
			h.countries[o.rec.Country] = struct{}{}
			h.ages[o.rec.Age] = struct{}{}
			h.scores[o.rec.Score] = struct{}{}
			h.actives[o.rec.Active] = struct{}{}
			for _, tag := range o.rec.Tags {
				h.tags[tag] = struct{}{}
			}
		case 1:
			for _, f := range o.patch {
				switch f.Name {
				case "name":
					h.names[f.Value.(string)] = struct{}{}
				case "email":
					h.emails[f.Value.(string)] = struct{}{}
				case "country":
					h.countries[f.Value.(string)] = struct{}{}
				case "age":
					h.ages[f.Value.(int)] = struct{}{}
				case "score":
					h.scores[f.Value.(float64)] = struct{}{}
				case "active":
					h.actives[f.Value.(bool)] = struct{}{}
				case "tags":
					for _, tag := range f.Value.([]string) {
						h.tags[tag] = struct{}{}
					}
				}
			}
		}
	}

	live := make(map[uint64]*Rec, idSpace)
	scanErr := readSeqScan(c, 0, func(id uint64, rec *Rec) (bool, error) {
		if rec == nil {
			return false, fmt.Errorf("nil record for id=%d", id)
		}
		live[id] = cloneRec(rec)
		return true, nil
	})
	if scanErr != nil {
		t.Fatalf("SeqScan: %v", scanErr)
	}

	count, err := readCount(c)
	if err != nil {
		t.Fatalf("Count(): %v", err)
	}
	if count != uint64(len(live)) {
		t.Fatalf("count mismatch: got=%d want=%d", count, len(live))
	}

	queryContainsID := func(q *qx.QX, id uint64) bool {
		ids, qerr := readQueryKeys(c, q)
		if qerr != nil {
			t.Fatalf("QueryKeys(%v): %v", q, qerr)
		}
		return slices.Contains(ids, id)
	}
	assertIndexContains := func(q *qx.QX, id uint64, desc string) {
		if !queryContainsID(q, id) {
			t.Fatalf("%s index missing id=%d", desc, id)
		}
	}
	assertIndexOmits := func(q *qx.QX, id uint64, desc string) {
		if queryContainsID(q, id) {
			t.Fatalf("stale %s index still contains id=%d", desc, id)
		}
	}

	for id := uint64(1); id <= idSpace; id++ {
		got, err := readGet(c, id)
		if err != nil {
			t.Fatalf("Get(%d): %v", id, err)
		}
		want := live[id]
		switch {
		case want == nil && got != nil:
			t.Fatalf("id=%d expected nil, got %#v", id, got)
		case want != nil && got == nil:
			t.Fatalf("id=%d expected value, got nil", id)
		case want != nil:
			if !reflect.DeepEqual(*got, *want) {
				t.Fatalf("id=%d payload mismatch\n got=%#v\nwant=%#v", id, got, want)
			}
		}

		fullName := fmt.Sprintf("ID-%03d", id)
		if want == nil {
			assertIndexOmits(qx.Query(qx.EQ("full_name", fullName)), id, fmt.Sprintf("full_name=%q", fullName))
			continue
		}

		assertIndexContains(qx.Query(qx.EQ("full_name", fullName)), id, fmt.Sprintf("full_name=%q", fullName))
		assertIndexContains(qx.Query(qx.EQ("name", want.Name)), id, fmt.Sprintf("name=%q", want.Name))
		assertIndexContains(qx.Query(qx.EQ("email", want.Email)), id, fmt.Sprintf("email=%q", want.Email))
		assertIndexContains(qx.Query(qx.EQ("country", want.Country)), id, fmt.Sprintf("country=%q", want.Country))
		assertIndexContains(qx.Query(qx.EQ("age", want.Age)), id, fmt.Sprintf("age=%d", want.Age))
		assertIndexContains(qx.Query(qx.EQ("score", want.Score)), id, fmt.Sprintf("score=%v", want.Score))
		assertIndexContains(qx.Query(qx.EQ("active", want.Active)), id, fmt.Sprintf("active=%v", want.Active))
		for _, tag := range want.Tags {
			assertIndexContains(qx.Query(qx.HASALL("tags", []string{tag})), id, fmt.Sprintf("tag=%q", tag))
		}
	}

	staleChecked := 0
	for id := uint64(1); id <= idSpace && staleChecked < staleProbe; id++ {
		h := historyByID[id]
		if h == nil {
			continue
		}
		cur := live[id]

		for name := range h.names {
			if staleChecked >= staleProbe {
				break
			}
			if cur != nil && cur.Name == name {
				continue
			}
			assertIndexOmits(qx.Query(qx.EQ("name", name)), id, fmt.Sprintf("name=%q", name))
			staleChecked++
		}
		for email := range h.emails {
			if staleChecked >= staleProbe {
				break
			}
			if cur != nil && cur.Email == email {
				continue
			}
			assertIndexOmits(qx.Query(qx.EQ("email", email)), id, fmt.Sprintf("email=%q", email))
			staleChecked++
		}
		for country := range h.countries {
			if staleChecked >= staleProbe {
				break
			}
			if cur != nil && cur.Country == country {
				continue
			}
			assertIndexOmits(qx.Query(qx.EQ("country", country)), id, fmt.Sprintf("country=%q", country))
			staleChecked++
		}
		for age := range h.ages {
			if staleChecked >= staleProbe {
				break
			}
			if cur != nil && cur.Age == age {
				continue
			}
			assertIndexOmits(qx.Query(qx.EQ("age", age)), id, fmt.Sprintf("age=%d", age))
			staleChecked++
		}
		for score := range h.scores {
			if staleChecked >= staleProbe {
				break
			}
			if cur != nil && cur.Score == score {
				continue
			}
			assertIndexOmits(qx.Query(qx.EQ("score", score)), id, fmt.Sprintf("score=%v", score))
			staleChecked++
		}
		for active := range h.actives {
			if staleChecked >= staleProbe {
				break
			}
			if cur != nil && cur.Active == active {
				continue
			}
			assertIndexOmits(qx.Query(qx.EQ("active", active)), id, fmt.Sprintf("active=%v", active))
			staleChecked++
		}
		for tag := range h.tags {
			if staleChecked >= staleProbe {
				break
			}
			if cur != nil && slices.Contains(cur.Tags, tag) {
				continue
			}
			assertIndexOmits(qx.Query(qx.HASALL("tags", []string{tag})), id, fmt.Sprintf("tag=%q", tag))
			staleChecked++
		}
	}
}
