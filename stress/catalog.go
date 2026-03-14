package main

import (
	"fmt"
	"slices"
)

type classDescriptor struct {
	Info        StressClassInfo
	Def         ClassDef
	QueryWeight map[string]float64
}

func loadClassCatalog() ([]*classDescriptor, map[int]*classDescriptor, map[string]*classDescriptor, error) {
	if err := validateClassCatalog(); err != nil {
		return nil, nil, nil, err
	}

	catalog := DefaultStressClassCatalog()
	defs := DefaultClassDefs()

	ordered := make([]*classDescriptor, 0, len(catalog))
	byID := make(map[int]*classDescriptor, len(catalog))
	byName := make(map[string]*classDescriptor, len(catalog)*2)

	for _, info := range catalog {
		def, ok := defs[info.Name]
		if !ok {
			return nil, nil, nil, fmt.Errorf("missing class def for %q", info.Name)
		}
		desc := &classDescriptor{
			Info: info,
			Def:  def,
		}
		if len(info.Queries) > 0 {
			desc.QueryWeight = make(map[string]float64, len(info.Queries))
			for _, query := range info.Queries {
				desc.QueryWeight[query.Name] = query.Weight
			}
		}
		ordered = append(ordered, desc)
		byID[info.ID] = desc
		byName[info.Name] = desc
		byName[info.Alias] = desc
	}

	slices.SortFunc(ordered, func(a, b *classDescriptor) int {
		return a.Info.ID - b.Info.ID
	})
	return ordered, byID, byName, nil
}
