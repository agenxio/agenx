package maps

type StrStructs map[string]struct{}

func MakeStrStructs(strings ...string) StrStructs {
	if len(strings) == 0 {
		return nil
	}

	set := StrStructs{}
	for _, str := range strings {
		set[str] = struct{}{}
	}
	return set
}

func (set StrStructs) Add(s string) {
	set[s] = struct{}{}
}

func (set StrStructs) Del(s string) {
	delete(set, s)
}

func (set StrStructs) Count() int {
	return len(set)
}

func (set StrStructs) Has(s string) (exists bool) {
	if set != nil {
		_, exists = set[s]
	}
	return
}