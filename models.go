package vectordb

import "webtyp.com/model"

type docRecord struct {
	ID      string
	Text    string
	Meta    model.RawJSON
	Tags    string
	Hash    string
	Created int64
	Hits    int64
	Shard   int64
	Slot    int64
}

func (d *docRecord) ModelName() string     { return DocModel.Name }
func (d *docRecord) Schema() []model.Field { return DocModel.Fields }
func (d *docRecord) Pointers() []any {
	return []any{&d.ID, &d.Text, &d.Meta, &d.Tags, &d.Hash, &d.Created, &d.Hits, &d.Shard, &d.Slot}
}
func (d *docRecord) IsNil() bool { return d == nil }
func (d *docRecord) EncodeFields(wr model.FieldWriter) {
	wr.String("id", d.ID)
	wr.String("text", d.Text)
	wr.Raw("meta", string(d.Meta))
	wr.String("tags", d.Tags)
	wr.String("hash", d.Hash)
	wr.Int("created", d.Created)
	wr.Int("hits", d.Hits)
	wr.Int("shard", d.Shard)
	wr.Int("slot", d.Slot)
}
func (d *docRecord) DecodeFields(r model.FieldReader) {
	if v, ok := r.String("id"); ok {
		d.ID = v
	}
	if v, ok := r.String("text"); ok {
		d.Text = v
	}
	if v, ok := r.Raw("meta"); ok {
		d.Meta = model.RawJSON(v)
	}
	if v, ok := r.String("tags"); ok {
		d.Tags = v
	}
	if v, ok := r.String("hash"); ok {
		d.Hash = v
	}
	if v, ok := r.Int("created"); ok {
		d.Created = v
	}
	if v, ok := r.Int("hits"); ok {
		d.Hits = v
	}
	if v, ok := r.Int("shard"); ok {
		d.Shard = v
	}
	if v, ok := r.Int("slot"); ok {
		d.Slot = v
	}
}

type shardRecord struct {
	ID    int64
	Count int64
	Data  []byte
}

func (s *shardRecord) ModelName() string     { return ShardModel.Name }
func (s *shardRecord) Schema() []model.Field { return ShardModel.Fields }
func (s *shardRecord) Pointers() []any {
	return []any{&s.ID, &s.Count, &s.Data}
}
func (s *shardRecord) IsNil() bool { return s == nil }
func (s *shardRecord) EncodeFields(wr model.FieldWriter) {
	wr.Int("id", s.ID)
	wr.Int("count", s.Count)
	wr.Bytes("data", s.Data)
}
func (s *shardRecord) DecodeFields(r model.FieldReader) {
	if v, ok := r.Int("id"); ok {
		s.ID = v
	}
	if v, ok := r.Int("count"); ok {
		s.Count = v
	}
	if v, ok := r.Bytes("data"); ok {
		s.Data = v
	}
}

type indexRecord struct {
	ID        string
	Dim       int64
	ModelID   string
	ShardSize int64
	Version   int64
}

func (i *indexRecord) ModelName() string     { return IndexModel.Name }
func (i *indexRecord) Schema() []model.Field { return IndexModel.Fields }
func (i *indexRecord) Pointers() []any {
	return []any{&i.ID, &i.Dim, &i.ModelID, &i.ShardSize, &i.Version}
}
func (i *indexRecord) IsNil() bool { return i == nil }
func (i *indexRecord) EncodeFields(wr model.FieldWriter) {
	wr.String("id", i.ID)
	wr.Int("dim", i.Dim)
	wr.String("model_id", i.ModelID)
	wr.Int("shard_size", i.ShardSize)
	wr.Int("version", i.Version)
}
func (i *indexRecord) DecodeFields(r model.FieldReader) {
	if v, ok := r.String("id"); ok {
		i.ID = v
	}
	if v, ok := r.Int("dim"); ok {
		i.Dim = v
	}
	if v, ok := r.String("model_id"); ok {
		i.ModelID = v
	}
	if v, ok := r.Int("shard_size"); ok {
		i.ShardSize = v
	}
	if v, ok := r.Int("version"); ok {
		i.Version = v
	}
}

var (
	_ model.Model = (*docRecord)(nil)
	_ model.Model = (*shardRecord)(nil)
	_ model.Model = (*indexRecord)(nil)
)
