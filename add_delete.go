package vectordb

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"time"

	"webtyp.com/context"
	"webtyp.com/storage"
	"webtyp.com/vector"
)

func hashText(text string) string {
	h := sha256.Sum256([]byte(text))
	return hex.EncodeToString(h[:])
}

func formatTags(tags []string) string {
	if len(tags) == 0 {
		return ""
	}
	var sb strings.Builder
	sb.WriteString("|")
	for _, t := range tags {
		t = strings.TrimSpace(t)
		if t != "" {
			sb.WriteString(t)
			sb.WriteString("|")
		}
	}
	res := sb.String()
	if res == "|" {
		return ""
	}
	return res
}

func (s *Store) Add(ctx *context.Context, docs ...Doc) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	dim := s.cfg.Embedder.Dim()
	var newDocs []Doc
	var newHashes []string

	for _, doc := range docs {
		hStr := hashText(doc.Text)
		if _, exists := s.docIDByHash(hStr); exists {
			continue // already in the corpus
		}
		if containsString(newHashes, hStr) {
			continue // duplicate within this same batch
		}
		if doc.ID == "" {
			doc.ID = s.cfg.IDGen.NewID()
		}
		newDocs = append(newDocs, doc)
		newHashes = append(newHashes, hStr)
	}

	if len(newDocs) == 0 {
		return nil, nil
	}

	texts := make([]string, len(newDocs))
	for i, d := range newDocs {
		texts[i] = d.Text
	}

	dst := make([]float32, len(newDocs)*dim)
	if err := s.cfg.Embedder.Embed(ctx, texts, dst); err != nil {
		return nil, err
	}

	// Prepare database execution
	var exec storage.Executor = s.cfg.Conn
	var tx storage.TxBoundExecutor
	if txExec, ok := s.cfg.Conn.(storage.TxExecutor); ok {
		t, err := txExec.BeginTx()
		if err != nil {
			return nil, err
		}
		defer t.Rollback()
		tx = t
		exec = t
	}

	var addedIDs []string
	now := time.Now().UnixNano()

	// Find or create active shard
	var activeShard *shardState
	var maxShardID int64
	for _, st := range s.shards {
		if st.id > maxShardID {
			maxShardID = st.id
		}
		if st.count < s.cfg.ShardSize && activeShard == nil {
			activeShard = st
		}
	}

	for i, doc := range newDocs {
		v := dst[i*dim : (i+1)*dim]
		if activeShard == nil || activeShard.count >= s.cfg.ShardSize {
			maxShardID++
			activeShard = &shardState{
				id:    maxShardID,
				count: 0,
				data:  nil,
				dirty: true,
			}
			s.shards = append(s.shards, activeShard)
		}

		slot := activeShard.count
		activeShard.data = append(activeShard.data, v...)
		activeShard.count++
		activeShard.dirty = true

		_, err := s.arena.Append(v)
		if err != nil {
			return nil, err
		}

		formattedTags := formatTags(doc.Tags)
		h := header{
			id:      doc.ID,
			tags:    formattedTags,
			created: now,
			hits:    0,
			deleted: false,
			shard:   activeShard.id,
			slot:    int64(slot),
			hash:    newHashes[i],
		}
		s.headers = append(s.headers, h)
		s.setHash(newHashes[i], doc.ID)

		dr := docRecord{
			ID:      doc.ID,
			Text:    doc.Text,
			Meta:    doc.Meta,
			Tags:    formattedTags,
			Hash:    newHashes[i],
			Created: h.created,
			Hits:    0,
			Shard:   h.shard,
			Slot:    h.slot,
		}

		qDoc := storage.Query{
			Action:  storage.ActionCreate,
			Table:   DocModel.Name,
			Columns: []string{"id", "text", "meta", "tags", "hash", "created", "hits", "shard", "slot"},
			Values:  []any{dr.ID, dr.Text, string(dr.Meta), dr.Tags, dr.Hash, dr.Created, dr.Hits, dr.Shard, dr.Slot},
		}
		pDoc, err := s.cfg.Conn.Compile(qDoc, &dr)
		if err != nil {
			return nil, err
		}
		if err := exec.Exec(pDoc.Query, pDoc.Args...); err != nil {
			return nil, err
		}

		addedIDs = append(addedIDs, doc.ID)
	}

	// Persist dirty shards
	for _, st := range s.shards {
		if !st.dirty {
			continue
		}
		sr := shardRecord{
			ID:    st.id,
			Count: int64(st.count),
			Data:  float32sToBytes(st.data),
		}

		// Check if shard exists on disk
		qReadShard := storage.Query{
			Action:     storage.ActionReadOne,
			Table:      ShardModel.Name,
			Conditions: []storage.Condition{storage.Eq("id", st.id)},
		}
		pRead, err := s.cfg.Conn.Compile(qReadShard, &sr)
		if err != nil {
			return nil, err
		}
		var existing shardRecord
		row := exec.QueryRow(pRead.Query, pRead.Args...)
		scanErr := row.Scan(&existing.ID, &existing.Count, &existing.Data)

		var qShard storage.Query
		if scanErr == storage.ErrNoRows {
			qShard = storage.Query{
				Action:  storage.ActionCreate,
				Table:   ShardModel.Name,
				Columns: []string{"id", "count", "data"},
				Values:  []any{sr.ID, sr.Count, sr.Data},
			}
		} else {
			qShard = storage.Query{
				Action:     storage.ActionUpdate,
				Table:      ShardModel.Name,
				Columns:    []string{"count", "data"},
				Values:     []any{sr.Count, sr.Data},
				Conditions: []storage.Condition{storage.Eq("id", st.id)},
			}
		}
		pShard, err := s.cfg.Conn.Compile(qShard, &sr)
		if err != nil {
			return nil, err
		}
		if err := exec.Exec(pShard.Query, pShard.Args...); err != nil {
			return nil, err
		}
		st.dirty = false
	}

	// Eviction must run BEFORE Commit, inside the same transaction (plan: "antes de
	// confirmar"). exec is the tx-bound executor while tx != nil; calling it again
	// after Commit would operate on an already-closed IndexedDB transaction -- silently
	// correct against mem (which does not enforce closure) but broken against a real
	// backend. Delete() already gets this ordering right; Add() did not.
	if err := s.evictIfNeeded(exec); err != nil {
		return nil, err
	}

	if tx != nil {
		if err := tx.Commit(); err != nil {
			return nil, err
		}
	}

	return addedIDs, nil
}

func (s *Store) Delete(ctx *context.Context, ids ...string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(ids) == 0 {
		return nil
	}

	// ids is already the membership list Delete needs — no reason to copy it
	// into a map just to ask "is this one of them".
	var affectedShards []int64
	var deletedIDs []string

	for i := range s.headers {
		h := &s.headers[i]
		if !h.deleted && containsString(ids, h.id) {
			h.deleted = true
			s.deleteHash(h.hash)
			if !containsInt64(affectedShards, h.shard) {
				affectedShards = append(affectedShards, h.shard)
			}
			deletedIDs = append(deletedIDs, h.id)
		}
	}

	if len(deletedIDs) == 0 {
		return nil
	}

	var exec storage.Executor = s.cfg.Conn
	var tx storage.TxBoundExecutor
	if txExec, ok := s.cfg.Conn.(storage.TxExecutor); ok {
		t, err := txExec.BeginTx()
		if err != nil {
			return err
		}
		defer t.Rollback()
		tx = t
		exec = t
	}

	for _, id := range deletedIDs {
		qDel := storage.Query{
			Action:     storage.ActionDelete,
			Table:      DocModel.Name,
			Conditions: []storage.Condition{storage.Eq("id", id)},
		}
		pDel, err := s.cfg.Conn.Compile(qDel, &docRecord{})
		if err != nil {
			return err
		}
		if err := exec.Exec(pDel.Query, pDel.Args...); err != nil {
			return err
		}
	}

	for _, sID := range affectedShards {
		if err := s.compactIfNeeded(exec, sID); err != nil {
			return err
		}
	}

	if tx != nil {
		if err := tx.Commit(); err != nil {
			return err
		}
	}

	return nil
}

func (s *Store) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	n := 0
	for _, h := range s.headers {
		if !h.deleted {
			n++
		}
	}
	return n
}

func (s *Store) evictIfNeeded(exec storage.Executor) error {
	dim := s.cfg.Embedder.Dim()
	docSize := int64(dim*4 + 64)

	activeCount := 0
	for _, h := range s.headers {
		if !h.deleted {
			activeCount++
		}
	}

	for {
		exceedsDocs := s.cfg.MaxDocs > 0 && activeCount > s.cfg.MaxDocs
		exceedsBytes := s.cfg.MaxBytes > 0 && int64(activeCount)*docSize > s.cfg.MaxBytes

		if !exceedsDocs && !exceedsBytes {
			break
		}
		if activeCount == 0 {
			break
		}

		// Find least used (hits ascending), then oldest (created ascending)
		minIdx := -1
		for i, h := range s.headers {
			if h.deleted {
				continue
			}
			if minIdx == -1 {
				minIdx = i
				continue
			}
			curr := s.headers[minIdx]
			if h.hits < curr.hits || (h.hits == curr.hits && h.created < curr.created) {
				minIdx = i
			}
		}

		if minIdx == -1 {
			break
		}

		evicted := &s.headers[minIdx]
		evicted.deleted = true
		s.deleteHash(evicted.hash)

		qDel := storage.Query{
			Action:     storage.ActionDelete,
			Table:      DocModel.Name,
			Conditions: []storage.Condition{storage.Eq("id", evicted.id)},
		}
		pDel, err := s.cfg.Conn.Compile(qDel, &docRecord{})
		if err != nil {
			return err
		}
		if err := exec.Exec(pDel.Query, pDel.Args...); err != nil {
			return err
		}

		if err := s.compactIfNeeded(exec, evicted.shard); err != nil {
			return err
		}

		activeCount--
	}

	return nil
}

func (s *Store) compactIfNeeded(exec storage.Executor, shardID int64) error {
	st := s.shardByID(shardID)
	if st == nil || st.count <= 1 {
		return nil
	}

	var activeHeaders []*header
	var totalInShard int
	for i := range s.headers {
		h := &s.headers[i]
		if h.shard == shardID {
			totalInShard++
			if !h.deleted {
				activeHeaders = append(activeHeaders, h)
			}
		}
	}

	// Compact if active items fall below half of total shard capacity
	if len(activeHeaders) >= totalInShard/2 {
		return nil
	}

	dim := s.cfg.Embedder.Dim()
	newData := make([]float32, len(activeHeaders)*dim)

	for newSlot, h := range activeHeaders {
		oldSlot := h.slot
		copy(newData[newSlot*dim:(newSlot+1)*dim], st.data[oldSlot*int64(dim):(oldSlot+1)*int64(dim)])
		h.slot = int64(newSlot)

		// Update vec_docs slot
		qUpdateDoc := storage.Query{
			Action:     storage.ActionUpdate,
			Table:      DocModel.Name,
			Columns:    []string{"slot"},
			Values:     []any{h.slot},
			Conditions: []storage.Condition{storage.Eq("id", h.id)},
		}
		pUpdateDoc, err := s.cfg.Conn.Compile(qUpdateDoc, &docRecord{})
		if err != nil {
			return err
		}
		if err := exec.Exec(pUpdateDoc.Query, pUpdateDoc.Args...); err != nil {
			return err
		}
	}

	st.data = newData
	st.count = len(activeHeaders)

	sr := shardRecord{
		ID:    st.id,
		Count: int64(st.count),
		Data:  float32sToBytes(st.data),
	}
	qShard := storage.Query{
		Action:     storage.ActionUpdate,
		Table:      ShardModel.Name,
		Columns:    []string{"count", "data"},
		Values:     []any{sr.Count, sr.Data},
		Conditions: []storage.Condition{storage.Eq("id", st.id)},
	}
	pShard, err := s.cfg.Conn.Compile(qShard, &sr)
	if err != nil {
		return err
	}
	if err := exec.Exec(pShard.Query, pShard.Args...); err != nil {
		return err
	}

	s.rebuildArenaAndHeaders()
	return nil
}

func (s *Store) rebuildArenaAndHeaders() {
	dim := s.cfg.Embedder.Dim()
	var newHeaders []header

	for _, h := range s.headers {
		if !h.deleted {
			newHeaders = append(newHeaders, h)
		}
	}

	newArena := vector.NewArena(dim, len(newHeaders))
	for _, h := range newHeaders {
		st := s.shardByID(h.shard)
		v := st.data[h.slot*int64(dim) : (h.slot+1)*int64(dim)]
		newArena.Append(v)
	}

	s.headers = newHeaders
	s.arena = newArena
}
