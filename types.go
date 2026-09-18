package vectordb

import (
	"sync"

	"webtyp.com/embed"
	"webtyp.com/fmt"
	"webtyp.com/model"
	"webtyp.com/storage"
	"webtyp.com/vector"
)

type Config struct {
	Conn      storage.Conn      // required — the backend, injected
	Embedder  embed.Embedder    // required — text → vectors
	IDGen     model.IDGenerator // required — no concrete generator constructed here
	ShardSize int               // default 1024
	MaxDocs   int               // 0 = unbounded; LRU eviction above this
	MaxBytes  int64             // 0 = derive from navigator.storage.estimate()
}

type Doc struct {
	ID   string // empty → IDGen.NewID()
	Text string
	Meta model.RawJSON
	Tags []string
}

type Query struct {
	Text        string    // embedded via Config.Embedder
	Vector      []float32 // pre-computed; takes precedence over Text
	K           int       // default 4, matching the TypeScript original
	IncludeTags []string  // AND
	ExcludeTags []string  // AND NOT
	MinScore    float32
}

type Match struct {
	Doc
	Score float32 // cosine similarity in [-1, 1]
}

type header struct {
	id      string
	tags    string // "|tag1|tag2|"
	created int64
	hits    int32
	deleted bool
	shard   int64
	slot    int64
	hash    string
}

type shardState struct {
	id    int64
	count int
	data  []float32
	dirty bool
}

// dirtyHit is a pending hit-count update for one document, flushed lazily
// (search path must not trigger a write). See Store.setDirtyHit.
type dirtyHit struct {
	docID string
	hits  int32
}

// No maps: shards stays a handful of entries even at the ~100k-document ceiling
// (D0), and a linear scan over it is free. hashes trades that same scan for the
// batch-insert complexity the master plan calls out in the TypeScript original
// (O(N·M) per batch) — accepted deliberately, revisit only if a real corpus makes
// it the bottleneck. dirtyHits is bounded by "distinct docs hit since last flush".
type Store struct {
	mu        sync.Mutex
	cfg       Config
	arena     *vector.Arena
	headers   []header
	shards    []*shardState
	hashes    []fmt.KeyValue // Key = content hash, Value = docID
	dirtyHits []dirtyHit
}

// findShardByID does the linear scan the map used to do in O(1). The shard list
// stays small on purpose (one entry per ShardSize documents), so this is cheap.
// A package-level func, not a method, because New builds the slice before a
// *Store exists to hang a method off of.
func findShardByID(shards []*shardState, id int64) *shardState {
	for _, st := range shards {
		if st.id == id {
			return st
		}
	}
	return nil
}

func (s *Store) shardByID(id int64) *shardState {
	return findShardByID(s.shards, id)
}

// docIDByHash reports the docID already stored under hash, if any.
func (s *Store) docIDByHash(hash string) (string, bool) {
	for _, kv := range s.hashes {
		if kv.Key == hash {
			return kv.Value, true
		}
	}
	return "", false
}

// setHash records a new hash → docID pair. Callers only ever insert a hash
// once (Add already checked docIDByHash first), so this never de-duplicates.
func (s *Store) setHash(hash, docID string) {
	s.hashes = append(s.hashes, fmt.KeyValue{Key: hash, Value: docID})
}

// deleteHash removes the pair for hash, if present.
func (s *Store) deleteHash(hash string) {
	for i, kv := range s.hashes {
		if kv.Key == hash {
			s.hashes = append(s.hashes[:i], s.hashes[i+1:]...)
			return
		}
	}
}

// setDirtyHit records docID's new total hit count, pending flush in Close.
func (s *Store) setDirtyHit(docID string, hits int32) {
	for i := range s.dirtyHits {
		if s.dirtyHits[i].docID == docID {
			s.dirtyHits[i].hits = hits
			return
		}
	}
	s.dirtyHits = append(s.dirtyHits, dirtyHit{docID: docID, hits: hits})
}

func containsString(list []string, v string) bool {
	for _, x := range list {
		if x == v {
			return true
		}
	}
	return false
}

func containsInt64(list []int64, v int64) bool {
	for _, x := range list {
		if x == v {
			return true
		}
	}
	return false
}
