package email

type Sender interface {
	Send(email, header, text string) error
}
