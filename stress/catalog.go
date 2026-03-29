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

func resolveInitialWorkers(catalog []*classDescriptor, explicit map[string]int, groups workerGroupOverrides) (map[string]int, error) {
	if err := validateInitialWorkersAgainstCatalog(catalog, explicit); err != nil {
		return nil, err
	}
	if groups.All.Set && countWorkerGroupMatches(catalog, "a") == 0 {
		return nil, fmt.Errorf("worker override for group %q matched no active classes", groupLabel("a"))
	}
	if groups.Read.Set && countWorkerGroupMatches(catalog, "r") == 0 {
		return nil, fmt.Errorf("worker override for group %q matched no active classes", groupLabel("r"))
	}
	if groups.Write.Set && countWorkerGroupMatches(catalog, "w") == 0 {
		return nil, fmt.Errorf("worker override for group %q matched no active classes", groupLabel("w"))
	}

	resolved := make(map[string]int, len(catalog))
	for _, class := range catalog {
		name := class.Info.Name
		if groups.All.Set {
			resolved[name] = groups.All.Value
		}
		if groups.Read.Set && workerGroupMatchesRole(class.Info.Role, "r") {
			resolved[name] = groups.Read.Value
		}
		if groups.Write.Set && workerGroupMatchesRole(class.Info.Role, "w") {
			resolved[name] = groups.Write.Value
		}
		if value, ok := explicit[name]; ok {
			resolved[name] = value
		}
	}
	return resolved, nil
}

func countWorkerGroupMatches(catalog []*classDescriptor, group string) int {
	count := 0
	for _, class := range catalog {
		if workerGroupMatchesRole(class.Info.Role, group) {
			count++
		}
	}
	return count
}
