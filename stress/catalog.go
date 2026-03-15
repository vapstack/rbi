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
	return loadFilteredClassCatalog(nil, nil)
}

func loadFilteredClassCatalog(classFilter, queryFilter []string) ([]*classDescriptor, map[int]*classDescriptor, map[string]*classDescriptor, error) {
	if err := validateClassCatalog(); err != nil {
		return nil, nil, nil, err
	}
	specs, err := filterStressClassSpecs(defaultStressClassSpecs(), classFilter, queryFilter)
	if err != nil {
		return nil, nil, nil, err
	}

	ordered := make([]*classDescriptor, 0, len(specs))
	byID := make(map[int]*classDescriptor, len(specs))
	byName := make(map[string]*classDescriptor, len(specs)*2)

	for _, spec := range specs {
		info := spec.info
		desc := &classDescriptor{
			Info: info,
			Def:  buildClassDef(spec),
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

func validateInitialWorkersAgainstCatalog(catalog []*classDescriptor, initial map[string]int) error {
	if len(initial) == 0 {
		return nil
	}
	active := make(map[string]struct{}, len(catalog))
	for _, class := range catalog {
		active[class.Info.Name] = struct{}{}
	}
	for name, n := range initial {
		if n <= 0 {
			continue
		}
		if _, ok := active[name]; ok {
			continue
		}
		return fmt.Errorf("worker override for filtered-out class %q", name)
	}
	return nil
}
