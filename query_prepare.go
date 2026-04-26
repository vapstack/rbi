package rbi

type preparedFieldResolver[K ~string | ~uint64, V any] struct {
	db *DB[K, V]
}

func (r preparedFieldResolver[K, V]) ResolveField(name string) (int, bool) {
	acc, ok := r.db.indexedFieldByName[name]
	if !ok {
		return 0, false
	}
	return acc.ordinal, true
}
