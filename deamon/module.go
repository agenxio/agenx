package scribe

import "sync"

type Module struct {
	sync.Mutex
	List map[uint64]Runner
}

func NewModule() *Module {
	return &Module{
		List: map[uint64]Runner{},
	}
}

func (r *Module) Add(hash uint64, m Runner) {
	r.Lock()
	defer r.Unlock()
	r.List[hash] = m
}

func (r *Module) Remove(hash uint64) {
	r.Lock()
	defer r.Unlock()
	delete(r.List, hash)
}

func (r *Module) Has(hash uint64) bool {
	r.Lock()
	defer r.Unlock()

	_, ok := r.List[hash]
	return ok
}

func (r *Module) CopyList() map[uint64]Runner {
	r.Lock()
	defer r.Unlock()

	// Create a copy of the list
	list := map[uint64]Runner{}
	for k, v := range r.List {
		list[k] = v
	}
	return list
}
