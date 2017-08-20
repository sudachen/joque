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

var testSet1 = []TestData{
	{"0 CONNECT\n\n", Message{ID: 0, Kind: MqConnect}},
	{"100 CONNECT\n\n", Message{ID: 100, Kind: MqConnect}},
	{"101 SUBSCRIBE test\n\n", Message{ID: 101, Kind: MqSubscribe, Topic: "test"}},
	{"102 SUBSCRIBE t$st\n\n", Message{ID: 102, Kind: MqSubscribe, Topic: "t$st"}},
	{"103 PUBLISH test 1 2\nq#12345\n\n", Message{ID: 103, Kind: MqPublish, Topic: "test", QoS: 1, Priority: 2, Payload: []byte("q#12345")}},
	{"104 PUBLISH t$st 0 0\nfjhtqrwytr1#$%789\n\n", Message{ID: 104, Kind: MqPublish, Topic: "t$st", QoS: 0, Priority: 0, Payload: []byte("fjhtqrwytr1#$%789")}},
	{"105 ACK\n\n", Message{ID: 105, Kind: MqAck}},
	{"106 QUERY\n\n", Message{ID: 106, Kind: MqQuery}},
	{"107 COMPLETE\n@#12345\n\n", Message{ID: 107, Kind: MqComplete, Payload: []byte("@#12345")}},
	{"108 PUBLISH test 1 2\n\n", Message{ID: 108, Kind: MqPublish, Topic: "test", QoS: 1, Priority: 2, Payload: nil}},
	{"109 COMPLETE\n\n", Message{ID: 109, Kind: MqComplete, Payload: nil}},
}

var testSet11 = []TestData{
	{"0 CONNECT \n\n", Message{ID: 0, Kind: MqConnect}},
	{"100   CONNECT\n\n", Message{ID: 100, Kind: MqConnect}},
	{"101 SUBSCRIBE   test\n\n", Message{ID: 101, Kind: MqSubscribe, Topic: "test"}},
	{"102   SUBSCRIBE t$st  \n\n", Message{ID: 102, Kind: MqSubscribe, Topic: "t$st"}},
	{"103 PUBLISH  test 1 2  \nq#12345\n\n", Message{ID: 103, Kind: MqPublish, Topic: "test", QoS: 1, Priority: 2, Payload: []byte("q#12345")}},
	{"104 PUBLISH t$st  0  0  \nfjhtqrwytr1#$%789\n\n", Message{ID: 104, Kind: MqPublish, Topic: "t$st", QoS: 0, Priority: 0, Payload: []byte("fjhtqrwytr1#$%789")}},
	{"  105   ACK  \n\n", Message{ID: 105, Kind: MqAck}},
	{"106  QUERY \n\n", Message{ID: 106, Kind: MqQuery}},
	{"107 COMPLETE \n@#12345\n\n", Message{ID: 107, Kind: MqComplete, Payload: []byte("@#12345")}},
	{"108 PUBLISH test  1 2  \n\n", Message{ID: 108, Kind: MqPublish, Topic: "test", QoS: 1, Priority: 2, Payload: nil}},
	{"109    COMPLETE  \n\n", Message{ID: 109, Kind: MqComplete, Payload: nil}},
}

var testSet2 = []string{
	"0 CONNECT 1\n\n",
	"CONNECT\n\n",
	"101 SUBSCRIBE\n\n",
	"102 SUBSCRIBE t$st\nqqqqq\n",
	"103 PUBLISH 1 2 test\nq#12345\n\n",
	"104 PUBLISH t$st\nfjhtqrwytr1#$%789\n\n",
	"105 ACK 1 2\n\n",
	"106 QUERY\n",
	"107 COMPLETE 2222\n@#12345\n\n",
	"106\n\n",
	"\n\n\n\n\n",
}

func TestAtoM1(t *testing.T) {
	mqt := &ASCIIMqt{}
	sets := [][]TestData{testSet1, testSet11}
	for _, ts := range sets {
		for _, d := range ts {
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
}

func TestAtoM2(t *testing.T) {
	mqt := &ASCIIMqt{}
	for _, s := range testSet2 {
		r := bytes.NewReader([]byte(s))
		_, err := mqt.MqtRead(r)

		if err == nil {
			t.Errorf("malformed message %s parsed successfull", s)
			t.Fail()
		}
	}
}

func TestAtoM3(t *testing.T) {
	mqt := &ASCIIMqt{}
	var bf bytes.Buffer
	for _, d := range testSet1 {
		bf.Write([]byte(d.text))
	}
	r := bytes.NewReader(bf.Bytes())
	for _, d := range testSet1 {
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

func TestMtoA1(t *testing.T) {
	mqt := &ASCIIMqt{}
	for _, d := range testSet1 {
		var bf bytes.Buffer
		err := mqt.MqtWrite(&bf, &d.m)
		if err != nil {
			t.Errorf("convert message %v to text: %s",
				d.m, err.Error())
			t.Fail()
		} else if bf.String() != d.text {
			t.Errorf("convert message %v:\n\tresult %s\n\texpected %s",
				d.m, strconv.Quote(bf.String()), strconv.Quote(d.text))
			t.Fail()
		}
	}
}
