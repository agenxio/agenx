package reader

type Limit struct {
	reader   Reader
	maxBytes int
}

func NewLimit(r Reader, maxBytes int) *Limit {
	return &Limit{reader: r, maxBytes: maxBytes}
}

func (p *Limit) Next() (Message, error) {
	message, err := p.reader.Next()
	if len(message.Content) > p.maxBytes {
		message.Content = message.Content[:p.maxBytes]
	}
	return message, err
}