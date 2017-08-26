package transport

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// ASCIIMqt is the ascii message queue transport
type ASCIIMqt struct {
	nl  bool
	bad bool
	eof bool
}

func (mqt *ASCIIMqt) readWord(rd io.Reader, bf *bytes.Buffer) (err error) {
	c := make([]byte, 1)
	bf.Reset()
	var n int
	for {
		if n, err = rd.Read(c); err != nil {
			return
		}
		if n <= 0 {
			err = errors.New("read from empty stream")
		}
		if c[0] != '\n' && c[0] != ' ' {
			bf.WriteByte(c[0])
		} else {
			mqt.nl = c[0] == '\n'
			if c[0] == '\n' || bf.Len() != 0 {
				return
			}
		}
	}
}

func (mqt *ASCIIMqt) readToNlNl(rd io.Reader, bf *bytes.Buffer) (err error) {
	c := make([]byte, 1)
	bf.Reset()
	var n int

	if !mqt.nl {
		for {
			if n, err = rd.Read(c); err != nil {
				return
			}
			if n <= 0 {
				err = errors.New("read from empty stream")
			}
			if c[0] == '\n' {
				mqt.nl = true
				break
			} else if c[0] != ' ' {
				err = errors.New("found non space byte")
				return
			}
		}
	}

	for {
		if n, err = rd.Read(c); err != nil {
			return
		}
		if n <= 0 {
			err = errors.New("read from empty stream")
		}

		if c[0] == '\n' {
			if mqt.nl {
				return
			}
			mqt.nl = true
		} else {
			if bf.Len() != 0 && mqt.nl {
				err = errors.New("found non NL byte")
				return
			}
			bf.WriteByte(c[0])
			mqt.nl = false
		}
	}
}

func (mqt *ASCIIMqt) inspectNlNl(rd io.Reader) (err error) {
	c := make([]byte, 1)
	var n int
	for {
		if n, err = rd.Read(c); err != nil {
			return
		}
		if n <= 0 {
			err = errors.New("read from empty stream")
		}

		if c[0] == '\n' {
			if mqt.nl {
				return
			}
			mqt.nl = true
		} else {
			if c[0] != ' ' || mqt.nl {
				err = errors.New("found non NL byte")
				return
			}
		}
	}
}

// MqtRead reads message form the byte stream
func (mqt *ASCIIMqt) MqtRead(rd io.Reader) (m *Message, err error) {
	var id int64
	var kind int
	var topic string
	var qos int
	var priority int
	var payload []byte
	var ttl int
	bf := &bytes.Buffer{}
	if err = mqt.readWord(rd, bf); err != nil {
		return
	}
	if id, err = strconv.ParseInt(bf.String(), 10, 64); err != nil {
		mqt.bad = true
		return
	}
	if err = mqt.readWord(rd, bf); err != nil {
		return
	}
	switch k := bf.String(); k {
	case "CONNECT":
		kind = MqConnect
	case "PUBLISH":
		kind = MqPublish
	case "SUBSCRIBE":
		kind = MqSubscribe
	case "ACK":
		kind = MqAck
	case "QUERY":
		kind = MqQuery
	case "COMPLETE":
		kind = MqComplete
	case "QUIT":
		kind = MqQuit
	default:
		err = fmt.Errorf("unknown message kind %s", k)
		mqt.bad = true
		return
	}
	if kind == MqPublish || kind == MqSubscribe {
		if err = mqt.readWord(rd, bf); err != nil {
			return
		}
		topic = bf.String()
	}
	if kind == MqPublish {
		if err = mqt.readWord(rd, bf); err != nil {
			return
		}
		if qos, err = strconv.Atoi(bf.String()); err != nil {
			mqt.bad = true
			return
		}
		if err = mqt.readWord(rd, bf); err != nil {
			return
		}
		if priority, err = strconv.Atoi(bf.String()); err != nil {
			mqt.bad = true
			return
		}
		if err = mqt.readWord(rd, bf); err != nil {
			return
		}
		if ttl, err = strconv.Atoi(bf.String()); err != nil {
			mqt.bad = true
			return
		}
	}
	if kind == MqPublish || kind == MqComplete {
		if err = mqt.readToNlNl(rd, bf); err != nil {
			return
		}
		if bf.Len() != 0 {
			payload = bf.Bytes()
		}
	} else {
		if err = mqt.inspectNlNl(rd); err != nil {
			return
		}
	}

	m = &Message{
		ID:       id,
		Kind:     kind,
		Topic:    topic,
		Payload:  payload,
		QoS:      qos,
		Priority: priority,
		TTL:      ttl,
	}
	return
}

// MqtWrite writes message to the byte stream
func (mqt *ASCIIMqt) MqtWrite(wr io.Writer, m *Message) (err error) {
	var bf bytes.Buffer
	var kind string
	if _, err = bf.WriteString(strconv.FormatInt(m.ID, 10)); err != nil {
		return
	}
	switch m.Kind {
	case MqConnect:
		kind = "CONNECT"
	case MqPublish:
		kind = "PUBLISH"
	case MqSubscribe:
		kind = "SUBSCRIBE"
	case MqAck:
		kind = "ACK"
	case MqQuery:
		kind = "QUERY"
	case MqComplete:
		kind = "COMPLETE"
	case MqQuit:
		kind = "QUIT"
	default:
		err = fmt.Errorf("invalid message kind %d", m.Kind)
		return
	}
	if err = bf.WriteByte(' '); err != nil {
		return
	}
	if _, err = bf.WriteString(kind); err != nil {
		return
	}
	if m.Kind == MqSubscribe || m.Kind == MqPublish {
		if len(m.Topic) == 0 {
			err = errors.New("message topic for COMPLETE and PUBLISH can't be empty")
			return
		}
		if strings.ContainsAny(m.Topic, "\n\t ") {
			err = errors.New("message topic is malformed")
			return
		}
		if err = bf.WriteByte(' '); err != nil {
			return
		}
		if _, err = bf.WriteString(m.Topic); err != nil {
			return
		}
	}
	if m.Kind == MqPublish {
		if err = bf.WriteByte(' '); err != nil {
			return
		}
		if _, err = bf.WriteString(strconv.Itoa(m.QoS)); err != nil {
			return
		}
		if err = bf.WriteByte(' '); err != nil {
			return
		}
		if _, err = bf.WriteString(strconv.Itoa(m.Priority)); err != nil {
			return
		}
		if err = bf.WriteByte(' '); err != nil {
			return
		}
		if _, err = bf.WriteString(strconv.Itoa(m.TTL)); err != nil {
			return
		}
	}
	if (m.Kind == MqComplete || m.Kind == MqPublish || m.Kind == MqAck) && m.Payload != nil && len(m.Payload) != 0 {
		if err = bf.WriteByte('\n'); err != nil {
			return
		}
		for _, c := range m.Payload {
			if c == '\n' {
				err = errors.New("message payload is malformed")
				return
			}
		}
		if _, err = bf.Write(m.Payload); err != nil {
			return
		}
	}
	if err = bf.WriteByte('\n'); err != nil {
		return
	}
	if err = bf.WriteByte('\n'); err != nil {
		return
	}
	_, err = wr.Write(bf.Bytes())
	return
}
