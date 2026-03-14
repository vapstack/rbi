package main

import "testing"

func TestLoadClassCatalogReadClassesHaveQueries(t *testing.T) {
	catalog, byID, _, err := loadClassCatalog()
	if err != nil {
		t.Fatalf("loadClassCatalog: %v", err)
	}
	if len(catalog) != 9 {
		t.Fatalf("catalog size = %d, want 9", len(catalog))
	}

	for id := 1; id <= 5; id++ {
		class := byID[id]
		if class == nil {
			t.Fatalf("missing class id %d", id)
		}
		if len(class.Info.Queries) == 0 {
			t.Fatalf("class %s has no queries", class.Info.Alias)
		}
	}

	if got := len(byID[1].Info.Queries); got < 2 {
		t.Fatalf("r_idx query count = %d, want at least 2", got)
	}
}
