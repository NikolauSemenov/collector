package broker

type MessageConsumer interface {
	ReadMessage() ([]byte, error)
	Commit() error
	Close() error
}
