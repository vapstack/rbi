package keycodec

import "github.com/vapstack/rbi/internal/pooled"

const (
	dataKeyPoolMaxCap  = 64 << 10
	indexKeyPoolMaxCap = 4 << 10
)

var (
	dataKeyPool  = pooled.NewSlicePool[DataKey](dataKeyPoolMaxCap, pooled.ClearCap)
	indexKeyPool = pooled.NewSlicePool[IndexKey](indexKeyPoolMaxCap, pooled.ClearCap)
)

func GetDataKeySlice(capHint int) []DataKey { return dataKeyPool.Get(capHint) }
func ReleaseDataKeySlice(buf []DataKey)     { dataKeyPool.Put(buf) }

func GetIndexKeySlice(capHint int) []IndexKey { return indexKeyPool.Get(capHint) }
func ReleaseIndexKeySlice(buf []IndexKey)     { indexKeyPool.Put(buf) }
