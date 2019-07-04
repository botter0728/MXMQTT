package stype

import (
	"github.com/eclipse/paho.mqtt.golang/packets"
	"net"
	"sync"
)

/***
*
* 定义结构体
 */
type MXMQTT struct {
	msgId       string
	clientId    string
	mutex       sync.Mutex
	queues      map[string]int
	status      int32
	tcpListener []net.Listener
	msgRouter   chan *msgRouter
	register    chan *register   //register session
	unregister  chan *unregister //unregister session
	onAccept    onAccept
}

//客户端连接注册
type register struct {
	//client  *Client
	connect *packets.ConnectPacket
	error   error
}

//客户端
type unregister struct {
	//client *Client
	done chan struct{}
}

type msgRouter struct {
	forceBroadcast bool
	clientIDs      map[string]struct{} //key by clientID
	pub            *packets.PublishPacket
}

type onAccept func(conn net.Conn) bool