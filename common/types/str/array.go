package str

import "strings"

type Array []string

func (a *Array) Set(s string) error {
	*a = append(*a, s)
	return nil
}

func (a *Array) String() string {
	return strings.Join(*a, ",")
}
