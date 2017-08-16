package mcg

// -----------------------------------------------------------------------------
// -- Agent
// -----------------------------------------------------------------------------

// Agent DOC: ..
type Agent interface {
	// Working with connections
	Connect() error
	Close()

	// Message Handling
	Send(string, *Message) error
	Receive(string, int, HandlerFunc) error
}
