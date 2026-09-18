package vectordb

import (
	"sync"

	"webtyp.com/embed"
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
	ID   string        // empty → IDGen.NewID()
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

type Store struct {
	mu        sync.Mutex
	cfg       Config
	arena     *vector.Arena
	headers   []header
	shards    map[int64]*shardState
	hashes    map[string]string // hash -> docID
	dirtyHits map[string]int32  // docID -> total hits
}
