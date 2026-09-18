package vectordb

import "webtyp.com/model"

// vec_docs — one row per document. Text and metadata are read ONLY for the final
// top-k, never during scoring (master index D2).
var DocModel = model.Definition{
	Name: "vec_docs",
	Fields: model.Fields{
		{Name: "id", Type: model.Text(), DB: &model.FieldDB{PK: true}},
		{Name: "text", Type: model.Text(), NotNull: true},
		{Name: "meta", Type: model.Raw()},                 // opaque JSON payload
		{Name: "tags", Type: model.Text()},                // "|a|b|c|", LIKE-filterable
		{Name: "hash", Type: model.Text(), NotNull: true}, // content hash, dedup
		{Name: "created", Type: model.Int(), NotNull: true},
		{Name: "hits", Type: model.Int(), NotNull: true},
		{Name: "shard", Type: model.Int(), NotNull: true},
		{Name: "slot", Type: model.Int(), NotNull: true},
	},
}

// vec_shards — one row per ShardSize vectors, as a single blob.
var ShardModel = model.Definition{
	Name: "vec_shards",
	Fields: model.Fields{
		{Name: "id", Type: model.Int(), DB: &model.FieldDB{PK: true}},
		{Name: "count", Type: model.Int(), NotNull: true},
		{Name: "data", Type: model.Blob(), NotNull: true},
	},
}

// vec_index — exactly one row. Rejects an arena that does not match the corpus.
var IndexModel = model.Definition{
	Name: "vec_index",
	Fields: model.Fields{
		{Name: "id", Type: model.Text(), DB: &model.FieldDB{PK: true}},
		{Name: "dim", Type: model.Int(), NotNull: true},
		{Name: "model_id", Type: model.Text(), NotNull: true}, // which embedder produced these
		{Name: "shard_size", Type: model.Int(), NotNull: true},
		{Name: "version", Type: model.Int(), NotNull: true},
	},
}
