//go:build wasm

package vectordb_test

import (
	"fmt"
	"sync/atomic"
	"testing"

	"webtyp.com/indexdb"
	"webtyp.com/model"
	"webtyp.com/storage"
	"webtyp.com/vectordb"
)

var dbSeq uint64

func freshDBName(t *testing.T) string {
	id := atomic.AddUint64(&dbSeq, 1)
	return fmt.Sprintf("testdb_%s_%d", t.Name(), id)
}

func toAny(models []model.Model) []any {
	anys := make([]any, len(models))
	for i, m := range models {
		anys[i] = m
	}
	return anys
}

func indexdbConn(t *testing.T) storage.Conn {
	return indexdb.New(freshDBName(t), &mockIDGen{}, nil, toAny(vectordb.Schema())...)
}

func TestIndexDB_Add_ThenSearchFindsIt(t *testing.T)   { testAdd_ThenSearchFindsIt(t, indexdbConn) }
func TestIndexDB_Add_DeduplicatesByHash(t *testing.T)  { testAdd_DeduplicatesByHash(t, indexdbConn) }
func TestIndexDB_Add_BatchOneTransaction(t *testing.T) { testAdd_BatchOneTransaction(t, indexdbConn) }
func TestIndexDB_Search_RanksByCosine(t *testing.T)    { testSearch_RanksByCosine(t, indexdbConn) }
func TestIndexDB_Search_RespectsK(t *testing.T)        { testSearch_RespectsK(t, indexdbConn) }
func TestIndexDB_Search_IncludeExcludeTags(t *testing.T) {
	testSearch_IncludeExcludeTags(t, indexdbConn)
}
func TestIndexDB_Search_MinScore(t *testing.T)    { testSearch_MinScore(t, indexdbConn) }
func TestIndexDB_Search_EmptyCorpus(t *testing.T) { testSearch_EmptyCorpus(t, indexdbConn) }
func TestIndexDB_Search_DoesNotRewriteCorpus(t *testing.T) {
	testSearch_DoesNotRewriteCorpus(t, indexdbConn)
}
func TestIndexDB_Reopen_LoadsIndex(t *testing.T) { testReopen_LoadsIndex(t, indexdbConn) }
func TestIndexDB_Reopen_ModelMismatchFails(t *testing.T) {
	testReopen_ModelMismatchFails(t, indexdbConn)
}
func TestIndexDB_Reopen_DimMismatchFails(t *testing.T) { testReopen_DimMismatchFails(t, indexdbConn) }
func TestIndexDB_Delete_RemovesFromResults(t *testing.T) {
	testDelete_RemovesFromResults(t, indexdbConn)
}
func TestIndexDB_Evict_LeastUsedOldestFirst(t *testing.T) {
	testEvict_LeastUsedOldestFirst(t, indexdbConn)
}
func TestIndexDB_Evict_CompactsShards(t *testing.T) { testEvict_CompactsShards(t, indexdbConn) }
func TestIndexDB_New_ReturnsBeforeUse(t *testing.T) { testNew_ReturnsBeforeUse(t, indexdbConn) }
