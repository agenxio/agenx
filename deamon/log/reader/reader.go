package reader

type Reader interface {
	Next() (Message, error)
}
