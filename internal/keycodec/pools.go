package keycodec

import "github.com/vapstack/pooled"

const (
	dataKeyPoolMaxCap        = 64 << 10
	indexKeyPoolMaxCap       = 4 << 10
	indexLookupKeyPoolMaxCap = 4 << 10
)

var (
	dataKeyPool        = pooled.Slices[DataKey]{MaxCap: dataKeyPoolMaxCap, Clear: pooled.ClearCap}
	indexKeyPool       = pooled.Slices[IndexKey]{MaxCap: indexKeyPoolMaxCap, Clear: pooled.ClearCap}
	indexLookupKeyPool = pooled.Slices[IndexLookupKey]{MaxCap: indexLookupKeyPoolMaxCap, Clear: pooled.ClearCap}
)

func GetDataKeySlice(capHint int) []DataKey { return dataKeyPool.Get(capHint) }
func ReleaseDataKeySlice(buf []DataKey)     { dataKeyPool.Put(buf) }

func GetIndexKeySlice(capHint int) []IndexKey { return indexKeyPool.Get(capHint) }
func ReleaseIndexKeySlice(buf []IndexKey)     { indexKeyPool.Put(buf) }

func GetIndexLookupKeySlice(capHint int) []IndexLookupKey { return indexLookupKeyPool.Get(capHint) }
func ReleaseIndexLookupKeySlice(buf []IndexLookupKey)     { indexLookupKeyPool.Put(buf) }
