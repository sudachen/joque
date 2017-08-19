package transport

import "io"

// ASCIIMqt is the ascii message queue transport
type ASCIIMqt struct {
}

// MqtRead reads message form the byte stream
func (mqt *ASCIIMqt) MqtRead(rd io.Reader) (m *Message, err error) {
	return
}

// MqtWrite writes message to the byte stream
func (mqt *ASCIIMqt) MqtWrite(wr io.Writer, m *Message) (err error) {
	return
}
