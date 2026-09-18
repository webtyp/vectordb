package vectordb

import (
	"encoding/binary"
	"math"

	"webtyp.com/context"
	"webtyp.com/fmt"
	"webtyp.com/model"
	"webtyp.com/storage"
	"webtyp.com/vector"
)

func float32sToBytes(fs []float32) []byte {
	b := make([]byte, len(fs)*4)
	for i, f := range fs {
		u := math.Float32bits(f)
		binary.LittleEndian.PutUint32(b[i*4:], u)
	}
	return b
}

func bytesToFloat32s(b []byte) []float32 {
	fs := make([]float32, len(b)/4)
	for i := range fs {
		u := binary.LittleEndian.Uint32(b[i*4 : (i+1)*4])
		fs[i] = math.Float32frombits(u)
	}
	return fs
}

// New opens the store and loads the index into memory. It RETURNS AN ERROR rather
// than racing a background load.
func New(ctx *context.Context, cfg Config) (*Store, error) {
	if cfg.Conn == nil {
		return nil, fmt.Err("vectordb: Conn is required")
	}
	if cfg.Embedder == nil {
		return nil, fmt.Err("vectordb: Embedder is required")
	}
	if cfg.IDGen == nil {
		return nil, fmt.Err("vectordb: IDGen is required")
	}
	if cfg.ShardSize <= 0 {
		cfg.ShardSize = 1024
	}

	initQuota()

	dim := cfg.Embedder.Dim()
	modelID := cfg.Embedder.ID()

	// 1. Read or initialize vec_index
	qIdx := storage.Query{
		Action:     storage.ActionReadOne,
		Table:      IndexModel.Name,
		Conditions: []storage.Condition{storage.Eq("id", "index")},
	}
	pIdx, err := cfg.Conn.Compile(qIdx, &indexRecord{})
	if err != nil {
		return nil, err
	}

	var idxRec indexRecord
	row := cfg.Conn.QueryRow(pIdx.Query, pIdx.Args...)
	err = row.Scan(&idxRec.ID, &idxRec.Dim, &idxRec.ModelID, &idxRec.ShardSize, &idxRec.Version)
	if err == storage.ErrNoRows {
		// New corpus: write index row
		idxRec = indexRecord{
			ID:        "index",
			Dim:       int64(dim),
			ModelID:   modelID,
			ShardSize: int64(cfg.ShardSize),
			Version:   1,
		}
		qCreateIdx := storage.Query{
			Action:  storage.ActionCreate,
			Table:   IndexModel.Name,
			Columns: []string{"id", "dim", "model_id", "shard_size", "version"},
			Values:  []any{idxRec.ID, idxRec.Dim, idxRec.ModelID, idxRec.ShardSize, idxRec.Version},
		}
		pCreateIdx, err := cfg.Conn.Compile(qCreateIdx, &idxRec)
		if err != nil {
			return nil, err
		}
		if err := cfg.Conn.Exec(pCreateIdx.Query, pCreateIdx.Args...); err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	} else {
		// Existing corpus: validate model_id and dim
		if idxRec.ModelID != modelID {
			return nil, fmt.Err("vectordb: model mismatch, corpus created with model ", idxRec.ModelID, " but embedder is ", modelID, "; corpus needs to be reindexed")
		}
		if idxRec.Dim != int64(dim) {
			return nil, fmt.Err("vectordb: dimension mismatch, corpus has dim ", idxRec.Dim, " but embedder has dim ", dim)
		}
		cfg.ShardSize = int(idxRec.ShardSize)
	}

	// 2. Read all vec_shards
	qShards := storage.Query{
		Action:  storage.ActionReadAll,
		Table:   ShardModel.Name,
		OrderBy: []storage.Order{storage.Asc("id")},
	}
	pShards, err := cfg.Conn.Compile(qShards, &shardRecord{})
	if err != nil {
		return nil, err
	}
	shardRows, err := cfg.Conn.Query(pShards.Query, pShards.Args...)
	if err != nil {
		return nil, err
	}
	defer shardRows.Close()

	var shardsSlice []*shardState
	var maxShardID int64
	for shardRows.Next() {
		var sr shardRecord
		if err := shardRows.Scan(&sr.ID, &sr.Count, &sr.Data); err != nil {
			return nil, err
		}
		if err := model.ValidateVector(ShardModel.Fields[2], sr.Data); err != nil {
			return nil, err
		}
		if len(sr.Data) > 0 && (sr.Count <= 0 || int64(len(sr.Data))/4/sr.Count != int64(dim)) {
			return nil, fmt.Err("vectordb: dimension mismatch in shard ", sr.ID)
		}
		dataFloat := bytesToFloat32s(sr.Data)
		shardsSlice = append(shardsSlice, &shardState{
			id:    sr.ID,
			count: int(sr.Count),
			data:  dataFloat,
			dirty: false,
		})
		if sr.ID > maxShardID {
			maxShardID = sr.ID
		}
	}

	// 3. Read all vec_docs
	qDocs := storage.Query{
		Action:  storage.ActionReadAll,
		Table:   DocModel.Name,
		OrderBy: []storage.Order{storage.Asc("shard"), storage.Asc("slot")},
	}
	pDocs, err := cfg.Conn.Compile(qDocs, &docRecord{})
	if err != nil {
		return nil, err
	}
	docRows, err := cfg.Conn.Query(pDocs.Query, pDocs.Args...)
	if err != nil {
		return nil, err
	}
	defer docRows.Close()

	// docs is read already sorted by (shard, slot) — the same order the shard/slot
	// walk below produces. No map/join structure is needed: both sequences are
	// sorted the same way, so a single cursor walked forward is a merge-join in
	// O(N), the join a map would give without needing one.
	var docs []docRecord
	for docRows.Next() {
		var dr docRecord
		if err := docRows.Scan(&dr.ID, &dr.Text, &dr.Meta, &dr.Tags, &dr.Hash, &dr.Created, &dr.Hits, &dr.Shard, &dr.Slot); err != nil {
			return nil, err
		}
		docs = append(docs, dr)
	}

	// 4. Build Arena, headers, hashes
	arena := vector.NewArena(dim, 0)
	var headers []header
	var hashes []fmt.KeyValue
	di := 0

	for sID := int64(1); sID <= maxShardID; sID++ {
		st := findShardByID(shardsSlice, sID)
		if st == nil {
			continue
		}
		for slot := 0; slot < st.count; slot++ {
			v := st.data[slot*dim : (slot+1)*dim]
			_, err := arena.Append(v)
			if err != nil {
				return nil, err
			}
			if di < len(docs) && docs[di].Shard == sID && docs[di].Slot == int64(slot) {
				dr := docs[di]
				di++
				hashes = append(hashes, fmt.KeyValue{Key: dr.Hash, Value: dr.ID})
				headers = append(headers, header{
					id:      dr.ID,
					tags:    dr.Tags,
					created: dr.Created,
					hits:    int32(dr.Hits),
					deleted: false,
					shard:   dr.Shard,
					slot:    dr.Slot,
					hash:    dr.Hash,
				})
			} else {
				headers = append(headers, header{
					deleted: true,
					shard:   sID,
					slot:    int64(slot),
				})
			}
		}
	}

	return &Store{
		cfg:       cfg,
		arena:     arena,
		headers:   headers,
		shards:    shardsSlice,
		hashes:    hashes,
		dirtyHits: nil,
	}, nil
}
