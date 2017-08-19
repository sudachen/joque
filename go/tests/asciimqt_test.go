package tests

import (
	"bytes"
	"reflect"
	"strconv"
	"testing"

	. "github.com/sudachen/joque/go/transport"
)

type TestData struct {
	text string
	m    Message
}

var testSet = []TestData{
	{"0 CONNECT\n\n", Message{ID: 0, Kind: MqConnect}},
	{"100 CONNECT\n\n", Message{ID: 100, Kind: MqConnect}},
	{"101 SUBSCRIBE test\n\n", Message{ID: 101, Kind: MqSubscribe, Topic: "test"}},
	{"102 SUBSCRIBE t$st\n\n", Message{ID: 102, Kind: MqSubscribe, Topic: "t$st"}},
	{"103 PUBLISH test 1 2\n@#12345\n\n", Message{ID: 103, Kind: MqPublish, Topic: "t$st", QoS: 1, Priority: 2, Payload: []byte("@#12345")}},
	{"104 PUBLISH t$st 0 0\nfjhtqrwytr1#$%789\n\n", Message{ID: 104, Kind: MqPublish, Topic: "t$st", QoS: 0, Priority: 0, Payload: []byte("nfjhtqrwytr1#$%789")}},
	{"105 ACK\n\n", Message{ID: 105, Kind: MqAck}},
	{"106 QUERY\n\n", Message{ID: 106, Kind: MqQuery}},
	{"107 COMPLETE\n@#12345\n", Message{ID: 107, Kind: MqComplete, Payload: []byte("@#12345")}},
}

func TestAtoM(t *testing.T) {
	mqt := &ASCIIMqt{}
	for _, d := range testSet {
		r := bytes.NewReader([]byte(d.text))
		m, err := mqt.MqtRead(r)
		if err != nil {
			t.Errorf("convert %s to message: %s",
				strconv.Quote(d.text), err.Error())
			t.Fail()
		} else if m == nil {
			t.Errorf("convert %s to message: nil",
				strconv.Quote(d.text))
			t.Fail()
		} else if !reflect.DeepEqual(*m, d.m) {
			t.Errorf("convert %s to message:\n\tresult %v\n\texpected %v",
				strconv.Quote(d.text), *m, d.m)
			t.Fail()
		}
	}
}
