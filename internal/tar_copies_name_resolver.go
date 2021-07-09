package internal

import (
	"fmt"
)

type TarCopiesNameResolver struct {
	tarPartIds     map[int]int
	copiedTarNames map[string]bool
	currTarId      int
}

func NewTarCopiesNameResolver() *TarCopiesNameResolver {
	return &TarCopiesNameResolver{make(map[int]int), make(map[string]bool), 1}
}

func (r *TarCopiesNameResolver) defaultName(id int, fileExtention string) string {
	return fmt.Sprintf("part_%0.3d.tar.%v", id, fileExtention)
}

func (r *TarCopiesNameResolver) AddCopiedTar(tarName string) {
	r.copiedTarNames[tarName] = true
}

func (r *TarCopiesNameResolver) ResolveByName(name string) string {
	if copied := r.copiedTarNames[name];copied {
		panic("Tar with this name already exists")
	}
	return name
}

func (r *TarCopiesNameResolver) ResolveByPart(part int, fileExtention string) string {
	if id, exists := r.tarPartIds[part];exists {
		return r.defaultName(id, fileExtention)
	}
	name := r.defaultName(r.currTarId, fileExtention)
	copied := r.copiedTarNames[name]
	for ;copied;r.currTarId += 1 {
		name = r.defaultName(r.currTarId, fileExtention)
		copied = r.copiedTarNames[name]
	}
	r.tarPartIds[part] = r.currTarId
	r.currTarId += 1
	return name
}