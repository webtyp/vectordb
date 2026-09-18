package vectordb_test

import (
	"strings"
	"sync"
	"testing"
	"time"

	"webtyp.com/context"
	"webtyp.com/embed"
	"webtyp.com/storage"
	"webtyp.com/storage/mem"
	"webtyp.com/vectordb"
)

type mockIDGen struct {
	mu  sync.Mutex
	seq int
}

func (m *mockIDGen) NewID() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.seq++
	return strings.ReplaceAll(time.Now().Format("150405.000000"), ".", "")
}

type fixedVectorEmbedder struct {
	dim     int
	id      string
	vectors map[string][]float32
}

func newFixedVectorEmbedder(dim int, id string) *fixedVectorEmbedder {
	return &fixedVectorEmbedder{
		dim:     dim,
		id:      id,
		vectors: make(map[string][]float32),
	}
}

func (f *fixedVectorEmbedder) Dim() int   { return f.dim }
func (f *fixedVectorEmbedder) ID() string { return f.id }

func (f *fixedVectorEmbedder) Embed(ctx *context.Context, texts []string, dst []float32) error {
	for i, text := range texts {
		vec, ok := f.vectors[text]
		if !ok {
			vec = make([]float32, f.dim)
			vec[0] = 1.0
		}
		copy(dst[i*f.dim:(i+1)*f.dim], vec)
	}
	return nil
}

func (f *fixedVectorEmbedder) Close() error { return nil }

type txRecorderConn struct {
	storage.Conn
	beginTxCount int
	commitCount  int
	execs        []string
}

func (c *txRecorderConn) BeginTx() (storage.TxBoundExecutor, error) {
	c.beginTxCount++
	txExec := c.Conn.(storage.TxExecutor)
	t, err := txExec.BeginTx()
	if err != nil {
		return nil, err
	}
	return &txRecorderBound{TxBoundExecutor: t, parent: c}, nil
}

func (c *txRecorderConn) Exec(query string, args ...any) error {
	c.execs = append(c.execs, query)
	return c.Conn.Exec(query, args...)
}

type txRecorderBound struct {
	storage.TxBoundExecutor
	parent *txRecorderConn
}

func (t *txRecorderBound) Exec(query string, args ...any) error {
	t.parent.execs = append(t.parent.execs, query)
	return t.TxBoundExecutor.Exec(query, args...)
}

func (t *txRecorderBound) Commit() error {
	t.parent.commitCount++
	return t.TxBoundExecutor.Commit()
}

func TestAdd_ThenSearchFindsIt(t *testing.T) {
	ctx := context.Background()
	conn := mem.New()
	emb := embed.NewMockEmbedder(16)
	idg := &mockIDGen{}

	store, err := vectordb.New(ctx, vectordb.Config{
		Conn:     conn,
		Embedder: emb,
		IDGen:    idg,
	})
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	ids, err := store.Add(ctx, vectordb.Doc{
		Text: "hello world",
		Tags: []string{"greeting"},
	})
	if err != nil {
		t.Fatalf("Add failed: %v", err)
	}
	if len(ids) != 1 {
		t.Fatalf("expected 1 ID, got %d", len(ids))
	}

	matches, err := store.Search(ctx, vectordb.Query{
		Text: "hello world",
		K:    1,
	})
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}
	if len(matches) != 1 {
		t.Fatalf("expected 1 match, got %d", len(matches))
	}
	if matches[0].ID != ids[0] {
		t.Errorf("match ID = %q, want %q", matches[0].ID, ids[0])
	}
	if matches[0].Text != "hello world" {
		t.Errorf("match Text = %q, want 'hello world'", matches[0].Text)
	}
}

func TestAdd_DeduplicatesByHash(t *testing.T) {
	ctx := context.Background()
	conn := mem.New()
	emb := embed.NewMockEmbedder(16)
	idg := &mockIDGen{}

	store, err := vectordb.New(ctx, vectordb.Config{
		Conn:     conn,
		Embedder: emb,
		IDGen:    idg,
	})
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	ids1, err := store.Add(ctx, vectordb.Doc{Text: "same text"})
	if err != nil || len(ids1) != 1 {
		t.Fatalf("first Add failed: %v, ids: %v", err, ids1)
	}

	ids2, err := store.Add(ctx, vectordb.Doc{Text: "same text"})
	if err != nil {
		t.Fatalf("second Add failed: %v", err)
	}
	if len(ids2) != 0 {
		t.Errorf("expected 0 IDs added on duplicate, got %d", len(ids2))
	}

	if store.Len() != 1 {
		t.Errorf("store.Len() = %d, want 1", store.Len())
	}
}

func TestAdd_BatchOneTransaction(t *testing.T) {
	ctx := context.Background()
	rawConn := mem.New()
	conn := &txRecorderConn{Conn: rawConn}
	emb := embed.NewMockEmbedder(16)
	idg := &mockIDGen{}

	store, err := vectordb.New(ctx, vectordb.Config{
		Conn:     conn,
		Embedder: emb,
		IDGen:    idg,
	})
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	var docs []vectordb.Doc
	for i := 0; i < 1024; i++ {
		docs = append(docs, vectordb.Doc{Text: string(rune(i)) + " text item"})
	}

	conn.beginTxCount = 0
	conn.commitCount = 0

	ids, err := store.Add(ctx, docs...)
	if err != nil {
		t.Fatalf("Add failed: %v", err)
	}
	if len(ids) != 1024 {
		t.Fatalf("expected 1024 added IDs, got %d", len(ids))
	}

	if conn.beginTxCount != 1 {
		t.Errorf("beginTxCount = %d, want 1", conn.beginTxCount)
	}
	if conn.commitCount != 1 {
		t.Errorf("commitCount = %d, want 1", conn.commitCount)
	}
}

func TestSearch_RanksByCosine(t *testing.T) {
	ctx := context.Background()
	conn := mem.New()
	emb := newFixedVectorEmbedder(3, "fixed")
	emb.vectors["doc1"] = []float32{1.0, 0.0, 0.0}
	emb.vectors["doc2"] = []float32{0.70710678, 0.70710678, 0.0}
	emb.vectors["doc3"] = []float32{0.0, 1.0, 0.0}
	emb.vectors["query"] = []float32{1.0, 0.0, 0.0}

	idg := &mockIDGen{}
	store, err := vectordb.New(ctx, vectordb.Config{
		Conn:     conn,
		Embedder: emb,
		IDGen:    idg,
	})
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	store.Add(ctx,
		vectordb.Doc{ID: "d1", Text: "doc1"},
		vectordb.Doc{ID: "d2", Text: "doc2"},
		vectordb.Doc{ID: "d3", Text: "doc3"},
	)

	matches, err := store.Search(ctx, vectordb.Query{
		Text: "query",
		K:    3,
	})
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}
	if len(matches) != 3 {
		t.Fatalf("expected 3 matches, got %d", len(matches))
	}

	if matches[0].ID != "d1" || matches[1].ID != "d2" || matches[2].ID != "d3" {
		t.Errorf("unexpected rank order: got [%s, %s, %s], want [d1, d2, d3]",
			matches[0].ID, matches[1].ID, matches[2].ID)
	}
}

func TestSearch_RespectsK(t *testing.T) {
	ctx := context.Background()
	conn := mem.New()
	emb := embed.NewMockEmbedder(16)
	idg := &mockIDGen{}

	store, err := vectordb.New(ctx, vectordb.Config{
		Conn:     conn,
		Embedder: emb,
		IDGen:    idg,
	})
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	var docs []vectordb.Doc
	for i := 0; i < 10; i++ {
		docs = append(docs, vectordb.Doc{Text: string(rune(i + 100))})
	}
	store.Add(ctx, docs...)

	matchesK3, err := store.Search(ctx, vectordb.Query{Text: "query", K: 3})
	if err != nil || len(matchesK3) != 3 {
		t.Fatalf("K=3 search failed: %v, len=%d", err, len(matchesK3))
	}

	matchesK20, err := store.Search(ctx, vectordb.Query{Text: "query", K: 20})
	if err != nil || len(matchesK20) != 10 {
		t.Fatalf("K=20 search failed: %v, len=%d", err, len(matchesK20))
	}
}

func TestSearch_IncludeExcludeTags(t *testing.T) {
	ctx := context.Background()
	conn := mem.New()
	emb := embed.NewMockEmbedder(16)
	idg := &mockIDGen{}

	store, err := vectordb.New(ctx, vectordb.Config{
		Conn:     conn,
		Embedder: emb,
		IDGen:    idg,
	})
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	store.Add(ctx,
		vectordb.Doc{ID: "d1", Text: "text 1", Tags: []string{"fruit", "red"}},
		vectordb.Doc{ID: "d2", Text: "text 2", Tags: []string{"fruit", "yellow"}},
		vectordb.Doc{ID: "d3", Text: "text 3", Tags: []string{"vegetable", "red"}},
	)

	// Include fruit
	res1, err := store.Search(ctx, vectordb.Query{Text: "text", IncludeTags: []string{"fruit"}, K: 10})
	if err != nil || len(res1) != 2 {
		t.Fatalf("Include fruit failed: %v, len=%d", err, len(res1))
	}

	// Include fruit, Exclude red
	res2, err := store.Search(ctx, vectordb.Query{Text: "text", IncludeTags: []string{"fruit"}, ExcludeTags: []string{"red"}, K: 10})
	if err != nil || len(res2) != 1 {
		t.Fatalf("Include fruit Exclude red failed: %v, len=%d", err, len(res2))
	}
	if res2[0].ID != "d2" {
		t.Errorf("expected d2, got %s", res2[0].ID)
	}
}

func TestSearch_MinScore(t *testing.T) {
	ctx := context.Background()
	conn := mem.New()
	emb := newFixedVectorEmbedder(3, "fixed")
	emb.vectors["doc1"] = []float32{1.0, 0.0, 0.0}
	emb.vectors["doc2"] = []float32{0.0, 1.0, 0.0}
	emb.vectors["query"] = []float32{1.0, 0.0, 0.0}

	idg := &mockIDGen{}
	store, err := vectordb.New(ctx, vectordb.Config{
		Conn:     conn,
		Embedder: emb,
		IDGen:    idg,
	})
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	store.Add(ctx,
		vectordb.Doc{ID: "d1", Text: "doc1"},
		vectordb.Doc{ID: "d2", Text: "doc2"},
	)

	matches, err := store.Search(ctx, vectordb.Query{
		Text:     "query",
		MinScore: 0.5,
		K:        10,
	})
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}
	if len(matches) != 1 {
		t.Fatalf("expected 1 match above MinScore 0.5, got %d", len(matches))
	}
	if matches[0].ID != "d1" {
		t.Errorf("expected d1, got %s", matches[0].ID)
	}
}

func TestSearch_EmptyCorpus(t *testing.T) {
	ctx := context.Background()
	conn := mem.New()
	emb := embed.NewMockEmbedder(16)
	idg := &mockIDGen{}

	store, err := vectordb.New(ctx, vectordb.Config{
		Conn:     conn,
		Embedder: emb,
		IDGen:    idg,
	})
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	matches, err := store.Search(ctx, vectordb.Query{Text: "empty"})
	if err != nil {
		t.Fatalf("Search on empty corpus returned error: %v", err)
	}
	if len(matches) != 0 {
		t.Errorf("expected 0 matches, got %d", len(matches))
	}
}

func TestSearch_DoesNotRewriteCorpus(t *testing.T) {
	ctx := context.Background()
	rawConn := mem.New()
	conn := &txRecorderConn{Conn: rawConn}
	emb := embed.NewMockEmbedder(16)
	idg := &mockIDGen{}

	store, err := vectordb.New(ctx, vectordb.Config{
		Conn:     conn,
		Embedder: emb,
		IDGen:    idg,
	})
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	store.Add(ctx, vectordb.Doc{Text: "persistent doc"})

	conn.execs = nil // reset query log

	_, err = store.Search(ctx, vectordb.Query{Text: "persistent doc"})
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}

	for _, q := range conn.execs {
		if strings.Contains(q, "vec_shards") {
			t.Errorf("Search triggered write to vec_shards: %s", q)
		}
	}
}

func TestReopen_LoadsIndex(t *testing.T) {
	ctx := context.Background()
	conn := mem.New()
	emb := embed.NewMockEmbedder(16)
	idg := &mockIDGen{}

	cfg := vectordb.Config{
		Conn:     conn,
		Embedder: emb,
		IDGen:    idg,
	}

	store, err := vectordb.New(ctx, cfg)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	store.Add(ctx,
		vectordb.Doc{ID: "d1", Text: "text 1"},
		vectordb.Doc{ID: "d2", Text: "text 2"},
	)
	store.Close()

	// Reopen on same Conn
	reopened, err := vectordb.New(ctx, cfg)
	if err != nil {
		t.Fatalf("Reopen failed: %v", err)
	}
	if reopened.Len() != 2 {
		t.Fatalf("reopened.Len() = %d, want 2", reopened.Len())
	}

	matches, err := reopened.Search(ctx, vectordb.Query{Text: "text 1"})
	if err != nil || len(matches) == 0 {
		t.Fatalf("Search after reopen failed: %v", err)
	}
}

func TestReopen_ModelMismatchFails(t *testing.T) {
	ctx := context.Background()
	conn := mem.New()
	idg := &mockIDGen{}

	embA := newFixedVectorEmbedder(16, "model_a")
	cfgA := vectordb.Config{
		Conn:     conn,
		Embedder: embA,
		IDGen:    idg,
	}

	store, err := vectordb.New(ctx, cfgA)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	store.Add(ctx, vectordb.Doc{Text: "doc text"})
	store.Close()

	embB := newFixedVectorEmbedder(16, "model_b")
	cfgB := vectordb.Config{
		Conn:     conn,
		Embedder: embB,
		IDGen:    idg,
	}

	_, err = vectordb.New(ctx, cfgB)
	if err == nil {
		t.Fatal("expected error on model mismatch, got nil")
	}
	if !strings.Contains(err.Error(), "reindex") {
		t.Errorf("error %q does not mention 'reindex'", err.Error())
	}
}

func TestReopen_DimMismatchFails(t *testing.T) {
	ctx := context.Background()
	conn := mem.New()
	idg := &mockIDGen{}

	embA := newFixedVectorEmbedder(16, "model_a")
	cfgA := vectordb.Config{
		Conn:     conn,
		Embedder: embA,
		IDGen:    idg,
	}

	store, err := vectordb.New(ctx, cfgA)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	store.Add(ctx, vectordb.Doc{Text: "doc text"})
	store.Close()

	embDiffDim := newFixedVectorEmbedder(32, "model_a")
	cfgDiffDim := vectordb.Config{
		Conn:     conn,
		Embedder: embDiffDim,
		IDGen:    idg,
	}

	_, err = vectordb.New(ctx, cfgDiffDim)
	if err == nil {
		t.Fatal("expected error on dim mismatch, got nil")
	}
}

func TestDelete_RemovesFromResults(t *testing.T) {
	ctx := context.Background()
	conn := mem.New()
	emb := embed.NewMockEmbedder(16)
	idg := &mockIDGen{}

	cfg := vectordb.Config{
		Conn:     conn,
		Embedder: emb,
		IDGen:    idg,
	}

	store, err := vectordb.New(ctx, cfg)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	store.Add(ctx,
		vectordb.Doc{ID: "d1", Text: "text 1"},
		vectordb.Doc{ID: "d2", Text: "text 2"},
	)

	if err := store.Delete(ctx, "d1"); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	if store.Len() != 1 {
		t.Fatalf("store.Len() = %d, want 1", store.Len())
	}

	matches, err := store.Search(ctx, vectordb.Query{Text: "text 1", K: 10})
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}
	for _, m := range matches {
		if m.ID == "d1" {
			t.Errorf("deleted doc d1 found in search results")
		}
	}

	store.Close()

	reopened, err := vectordb.New(ctx, cfg)
	if err != nil {
		t.Fatalf("Reopen failed: %v", err)
	}
	if reopened.Len() != 1 {
		t.Fatalf("reopened.Len() = %d, want 1", reopened.Len())
	}
}

func TestEvict_LeastUsedOldestFirst(t *testing.T) {
	ctx := context.Background()
	conn := mem.New()
	emb := embed.NewMockEmbedder(16)
	idg := &mockIDGen{}

	store, err := vectordb.New(ctx, vectordb.Config{
		Conn:     conn,
		Embedder: emb,
		IDGen:    idg,
		MaxDocs:  2,
	})
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	// Add d1
	store.Add(ctx, vectordb.Doc{ID: "d1", Text: "doc 1"})
	time.Sleep(10 * time.Millisecond)

	// Search d1 to increase hits
	store.Search(ctx, vectordb.Query{Text: "doc 1"})

	// Add d2
	store.Add(ctx, vectordb.Doc{ID: "d2", Text: "doc 2"})
	time.Sleep(10 * time.Millisecond)

	// Add d3 -> triggers eviction since MaxDocs = 2
	store.Add(ctx, vectordb.Doc{ID: "d3", Text: "doc 3"})

	if store.Len() != 2 {
		t.Fatalf("store.Len() = %d, want 2", store.Len())
	}

	// d1 has 1 hit, d2 has 0 hits (older than d3), d3 has 0 hits.
	// d2 should be evicted!
	matches, err := store.Search(ctx, vectordb.Query{Text: "doc", K: 10})
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}

	foundD2 := false
	for _, m := range matches {
		if m.ID == "d2" {
			foundD2 = true
		}
	}
	if foundD2 {
		t.Errorf("d2 was expected to be evicted, but was found in search results")
	}
}

func TestEvict_CompactsShards(t *testing.T) {
	ctx := context.Background()
	conn := mem.New()
	emb := embed.NewMockEmbedder(16)
	idg := &mockIDGen{}

	store, err := vectordb.New(ctx, vectordb.Config{
		Conn:      conn,
		Embedder:  emb,
		IDGen:     idg,
		ShardSize: 10,
	})
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	var docs []vectordb.Doc
	for i := 1; i <= 10; i++ {
		docs = append(docs, vectordb.Doc{Text: "item " + string(rune(i+64))})
	}
	ids, err := store.Add(ctx, docs...)
	if err != nil || len(ids) != 10 {
		t.Fatalf("Add 10 docs failed: %v", err)
	}

	// Delete 6 items to trigger compaction (less than half capacity)
	deleteIDs := ids[:6]
	if err := store.Delete(ctx, deleteIDs...); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	if store.Len() != 4 {
		t.Fatalf("store.Len() = %d, want 4", store.Len())
	}

	matches, err := store.Search(ctx, vectordb.Query{Text: "item", K: 10})
	if err != nil {
		t.Fatalf("Search after compaction failed: %v", err)
	}
	if len(matches) != 4 {
		t.Errorf("matches len = %d, want 4", len(matches))
	}
}

func TestNew_ReturnsBeforeUse(t *testing.T) {
	ctx := context.Background()
	conn := mem.New()
	emb := embed.NewMockEmbedder(16)
	idg := &mockIDGen{}

	store, err := vectordb.New(ctx, vectordb.Config{
		Conn:     conn,
		Embedder: emb,
		IDGen:    idg,
	})
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	// Immediately search or check Len on fresh return
	if n := store.Len(); n != 0 {
		t.Errorf("store.Len() = %d, want 0", n)
	}

	matches, err := store.Search(ctx, vectordb.Query{Text: "query"})
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}
	if len(matches) != 0 {
		t.Errorf("matches len = %d, want 0", len(matches))
	}
}
