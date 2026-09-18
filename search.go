package vectordb

import (
	"strings"

	"webtyp.com/context"
	"webtyp.com/fmt"
	"webtyp.com/storage"
	"webtyp.com/vector"
)

func parseTags(tagsStr string) []string {
	if tagsStr == "" || tagsStr == "|" {
		return nil
	}
	parts := strings.Split(tagsStr, "|")
	var res []string
	for _, p := range parts {
		if p != "" {
			res = append(res, p)
		}
	}
	return res
}

func (s *Store) Search(ctx *context.Context, q Query) ([]Match, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	dim := s.cfg.Embedder.Dim()
	if s.arena.Len() == 0 {
		return nil, nil
	}

	var queryVec []float32
	if len(q.Vector) > 0 {
		if len(q.Vector) != dim {
			return nil, fmt.Err("vectordb: query vector dimension mismatch, expected ", dim, " got ", len(q.Vector))
		}
		queryVec = make([]float32, dim)
		copy(queryVec, q.Vector)
	} else if q.Text != "" {
		queryVec = make([]float32, dim)
		if err := s.cfg.Embedder.Embed(ctx, []string{q.Text}, queryVec); err != nil {
			return nil, err
		}
	} else {
		return nil, nil
	}

	vector.Normalize(queryVec)

	k := q.K
	if k <= 0 {
		k = 4
	}

	keep := func(i int) bool {
		if i < 0 || i >= len(s.headers) {
			return false
		}
		h := s.headers[i]
		if h.deleted {
			return false
		}
		if len(q.IncludeTags) > 0 {
			for _, tag := range q.IncludeTags {
				if !strings.Contains(h.tags, "|"+tag+"|") {
					return false
				}
			}
		}
		if len(q.ExcludeTags) > 0 {
			for _, tag := range q.ExcludeTags {
				if strings.Contains(h.tags, "|"+tag+"|") {
					return false
				}
			}
		}
		return true
	}

	topk := vector.NewTopK(k)
	s.arena.Search(queryVec, keep, topk)
	topMatches := topk.Results(nil)

	if len(topMatches) == 0 {
		return nil, nil
	}

	var winning []vector.Match
	for _, m := range topMatches {
		if q.MinScore > 0 && m.Score < q.MinScore {
			continue
		}
		winning = append(winning, m)
	}

	if len(winning) == 0 {
		return nil, nil
	}

	var results []Match
	for _, m := range winning {
		h := &s.headers[m.ID]
		h.hits++
		s.setDirtyHit(h.id, h.hits)

		qDoc := storage.Query{
			Action:     storage.ActionReadOne,
			Table:      DocModel.Name,
			Conditions: []storage.Condition{storage.Eq("id", h.id)},
		}
		pDoc, err := s.cfg.Conn.Compile(qDoc, &docRecord{})
		if err != nil {
			return nil, err
		}

		var dr docRecord
		row := s.cfg.Conn.QueryRow(pDoc.Query, pDoc.Args...)
		if err := row.Scan(&dr.ID, &dr.Text, &dr.Meta, &dr.Tags, &dr.Hash, &dr.Created, &dr.Hits, &dr.Shard, &dr.Slot); err != nil {
			return nil, err
		}

		results = append(results, Match{
			Doc: Doc{
				ID:   dr.ID,
				Text: dr.Text,
				Meta: dr.Meta,
				Tags: parseTags(dr.Tags),
			},
			Score: m.Score,
		})
	}

	return results, nil
}

func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, dh := range s.dirtyHits {
		qUpdate := storage.Query{
			Action:     storage.ActionUpdate,
			Table:      DocModel.Name,
			Columns:    []string{"hits"},
			Values:     []any{int64(dh.hits)},
			Conditions: []storage.Condition{storage.Eq("id", dh.docID)},
		}
		pUpdate, err := s.cfg.Conn.Compile(qUpdate, &docRecord{})
		if err != nil {
			return err
		}
		// Flushing hit counts must not mask a real close-time failure: the
		// caller needs to know if the last write to the backend didn't land.
		if err := s.cfg.Conn.Exec(pUpdate.Query, pUpdate.Args...); err != nil {
			return err
		}
	}
	s.dirtyHits = nil

	return s.cfg.Conn.Close()
}
