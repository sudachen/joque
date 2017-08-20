package server

import (
	"net"
	"time"

	"github.com/golang/glog"
	"github.com/sudachen/joque/go/broker"
	"github.com/sudachen/joque/go/transport"
)

// JoqueServer is the TCP job queuing service
type JoqueServer struct {
	chStop chan int
}

// Stop stops server
func (srv *JoqueServer) Stop() {
	defer recover()
	srv.chStop <- 0
	<-srv.chStop
}

// StartJoqueServer starts Joque broker on specified tcp host:port
func StartJoqueServer(where string, maxQueLength int, maxTTL int) (srv *JoqueServer, err error) {

	glog.Infof("starting joque server")
	srv = &JoqueServer{make(chan int)}
	c := make(chan error)

	go func() {
		tcpAddr, err := net.ResolveTCPAddr("tcp", where)
		if err != nil {
			c <- err
			return
		}
		l, err := net.ListenTCP("tcp", tcpAddr)
		if err != nil {
			c <- err
			return
		}

		brk := broker.StartJoqueBroker(maxQueLength)

		defer func() {
			glog.Infof("stopping joque server")
			l.Close()
			brk.Stop()
			close(srv.chStop)
			glog.Infof("joque server stopped")
		}()

		c <- nil
		for {
			l.SetDeadline(time.Now().Add(100 * time.Millisecond))
			conn, err := l.Accept()
		chkerr:
			switch err {
			case nil:
			default:
				switch err := err.(type) {
				case net.Error:
					if err.Timeout() {
						break chkerr
					}
				}
				glog.Errorf("tcp accept failed: %s", err.Error())
				return
			}
			if conn != nil {
				Connect(transport.Upgrade(conn, new(transport.ASCIIMqt)), brk, maxTTL)
			}
			select {
			case <-srv.chStop:
				return
			}
		}
	}()

	err = <-c
	if err == nil {
		glog.Infof("joque server started")
	}
	return
}
