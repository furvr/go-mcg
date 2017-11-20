package mcg

// -----------------------------------------------------------------------------

type Message struct {
	Retries int     `json:"retries"`
	Context Context `json:"data"`
	Body    *Body   `json:"body"`
}

// -----------------------------------------------------------------------------

type Context map[string]interface{}

// -----------------------------------------------------------------------------

type Body struct {
	Parts  []Part
	cursor int
}

func NewBodyFromString(message string) Body {
	var body = Body{Parts: make([]Part, 0)}
	body.AddPart(Part(message))
	return body
}

func NewBodyFromStrings(messages []string) Body {
	var body = Body{Parts: make([]Part, 0)}

	for _, msg := range messages {
		body.AddPart(Part(msg))
	}

	return body
}

// ---

func (b *Body) AddPart(part Part) {
	b.Parts = append(b.Parts, part)
}

// -----------------------------------------------------------------------------

type Part []byte
