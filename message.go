package mcg

// ---

// Message DOC: ..
type Message struct {
	Retries int  `json:"retries"`
	Data    Data `json:"data"`
}

// Data DOC: ..
type Data interface{}
