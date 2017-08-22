package mcg

// -----------------------------------------------------------------------------
// -- Agent
// -----------------------------------------------------------------------------

// Agent DOC: ..
type Agent interface {
	// Working with connections
	Connect() error
	Close() error
	Done() error

	// Message Handling
	Send(string, string, *Message) error
	Receive(string, string, int, HandlerFunc) error
}
