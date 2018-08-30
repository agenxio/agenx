package reader

type StripNewline struct {
	reader Reader
}

func NewStripNewline(r Reader) *StripNewline {
	return &StripNewline{r}
}

func (p *StripNewline) Next() (Message, error) {
	message, err := p.reader.Next()
	if err != nil {
		return message, err
	}

	L := message.Content
	message.Content = L[:len(L)-lineEndingChars(L)]
	return message, err
}

func isLine(l []byte) bool {
	return l != nil && len(l) > 0 && l[len(l)-1] == '\n'
}

func lineEndingChars(l []byte) int {
	if !isLine(l) {
		return 0
	}

	if len(l) > 1 && l[len(l)-2] == '\r' {
		return 2
	}
	return 1
}