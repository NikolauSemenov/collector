package broker

type MessageConsumer interface {
	ReadMessage() ([]byte, error)
	Close() error
}
