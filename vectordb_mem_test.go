package vectordb_test

import (
	"testing"

	"webtyp.com/storage"
	"webtyp.com/storage/mem"
)

func memConn(t *testing.T) storage.Conn {
	return mem.New()
}

func TestAdd_ThenSearchFindsIt(t *testing.T)       { testAdd_ThenSearchFindsIt(t, memConn) }
func TestAdd_DeduplicatesByHash(t *testing.T)       { testAdd_DeduplicatesByHash(t, memConn) }
func TestAdd_BatchOneTransaction(t *testing.T)      { testAdd_BatchOneTransaction(t, memConn) }
func TestSearch_RanksByCosine(t *testing.T)         { testSearch_RanksByCosine(t, memConn) }
func TestSearch_RespectsK(t *testing.T)             { testSearch_RespectsK(t, memConn) }
func TestSearch_IncludeExcludeTags(t *testing.T)    { testSearch_IncludeExcludeTags(t, memConn) }
func TestSearch_MinScore(t *testing.T)              { testSearch_MinScore(t, memConn) }
func TestSearch_EmptyCorpus(t *testing.T)           { testSearch_EmptyCorpus(t, memConn) }
func TestSearch_DoesNotRewriteCorpus(t *testing.T) { testSearch_DoesNotRewriteCorpus(t, memConn) }
func TestReopen_LoadsIndex(t *testing.T)            { testReopen_LoadsIndex(t, memConn) }
func TestReopen_ModelMismatchFails(t *testing.T)    { testReopen_ModelMismatchFails(t, memConn) }
func TestReopen_DimMismatchFails(t *testing.T)      { testReopen_DimMismatchFails(t, memConn) }
func TestDelete_RemovesFromResults(t *testing.T)    { testDelete_RemovesFromResults(t, memConn) }
func TestEvict_LeastUsedOldestFirst(t *testing.T)   { testEvict_LeastUsedOldestFirst(t, memConn) }
func TestEvict_CompactsShards(t *testing.T)         { testEvict_CompactsShards(t, memConn) }
func TestNew_ReturnsBeforeUse(t *testing.T)         { testNew_ReturnsBeforeUse(t, memConn) }
