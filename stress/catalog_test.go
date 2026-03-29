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

func TestLoadFilteredClassCatalogByClassAlias(t *testing.T) {
	catalog, _, byName, err := loadFilteredClassCatalog([]string{"r_med"}, nil)
	if err != nil {
		t.Fatalf("loadFilteredClassCatalog: %v", err)
	}
	if len(catalog) != 1 {
		t.Fatalf("catalog size = %d, want 1", len(catalog))
	}
	if catalog[0].Info.Alias != "r_med" {
		t.Fatalf("alias = %q, want r_med", catalog[0].Info.Alias)
	}
	if byName["r_med"] == nil || byName["read_medium"] == nil {
		t.Fatalf("filtered catalog missing aliases/names")
	}
}

func TestLoadFilteredClassCatalogByQueryNormalizesWeights(t *testing.T) {
	catalog, _, _, err := loadFilteredClassCatalog(nil, []string{"read_leaderboard_top_items"})
	if err != nil {
		t.Fatalf("loadFilteredClassCatalog: %v", err)
	}
	if len(catalog) != 1 {
		t.Fatalf("catalog size = %d, want 1", len(catalog))
	}
	class := catalog[0]
	if class.Info.Alias != "r_med" {
		t.Fatalf("alias = %q, want r_med", class.Info.Alias)
	}
	if len(class.Info.Queries) != 1 {
		t.Fatalf("query count = %d, want 1", len(class.Info.Queries))
	}
	if class.Info.Queries[0].Name != "read_leaderboard_top_items" {
		t.Fatalf("query = %q, want read_leaderboard_top_items", class.Info.Queries[0].Name)
	}
	if class.Info.Queries[0].Weight != 1 {
		t.Fatalf("weight = %v, want 1", class.Info.Queries[0].Weight)
	}
	if got := class.QueryWeight["read_leaderboard_top_items"]; got != 1 {
		t.Fatalf("query weight = %v, want 1", got)
	}
}

func TestLoadFilteredClassCatalogRejectsUnknownQuery(t *testing.T) {
	if _, _, _, err := loadFilteredClassCatalog(nil, []string{"does_not_exist"}); err == nil {
		t.Fatal("expected unknown query filter error")
	}
}

func TestValidateInitialWorkersAgainstCatalogRejectsFilteredOutClass(t *testing.T) {
	catalog, _, _, err := loadFilteredClassCatalog([]string{"r_med"}, nil)
	if err != nil {
		t.Fatalf("loadFilteredClassCatalog: %v", err)
	}
	err = validateInitialWorkersAgainstCatalog(catalog, map[string]int{
		ClassReadMedium: 4,
		ClassReadHeavy:  1,
	})
	if err == nil {
		t.Fatal("expected worker override validation error")
	}
}

func TestResolveInitialWorkersAppliesGroupOverridesAfterFiltering(t *testing.T) {
	catalog, _, _, err := loadFilteredClassCatalog([]string{"r_med", "w_fst"}, nil)
	if err != nil {
		t.Fatalf("loadFilteredClassCatalog: %v", err)
	}

	initial, err := resolveInitialWorkers(catalog, map[string]int{
		ClassReadMedium: 7,
		ClassWriteFast:  0,
	}, workerGroupOverrides{
		All:   workerCountOverride{Value: 1, Set: true},
		Read:  workerCountOverride{Value: 10, Set: true},
		Write: workerCountOverride{Value: 5, Set: true},
	})
	if err != nil {
		t.Fatalf("resolveInitialWorkers: %v", err)
	}

	if got := initial[ClassReadMedium]; got != 7 {
		t.Fatalf("read_medium workers = %d, want 7", got)
	}
	value, ok := initial[ClassWriteFast]
	if !ok || value != 0 {
		t.Fatalf("write_fast workers = (%d, %t), want (0, true)", value, ok)
	}
	if len(initial) != 2 {
		t.Fatalf("resolved initial workers size = %d, want 2", len(initial))
	}
}

func TestResolveInitialWorkersRejectsEmptyFilteredGroup(t *testing.T) {
	catalog, _, _, err := loadFilteredClassCatalog([]string{"w_fst"}, nil)
	if err != nil {
		t.Fatalf("loadFilteredClassCatalog: %v", err)
	}

	_, err = resolveInitialWorkers(catalog, nil, workerGroupOverrides{
		Read: workerCountOverride{Value: 4, Set: true},
	})
	if err == nil {
		t.Fatal("expected read group resolution error")
	}
}
