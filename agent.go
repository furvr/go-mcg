package mcg

// -----------------------------------------------------------------------------
// -- Agent
// -----------------------------------------------------------------------------

// Agent DOC: ..
type Agent interface {
	Connect() error
	Close()
}
