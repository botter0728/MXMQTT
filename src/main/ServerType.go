package main

import (
	"github.com/eclipse/paho.mqtt.golang/packets"
	"net"
	"sync"
	"time"
)

/***
*
* 定义结构体
 */
type MXMQTT struct {
	msgId       string
	connProxyId string
	retainedMsgMu       sync.Mutex
	rwMu             sync.RWMutex   //读写
	retainedMsg     map[string]*packets.PublishPacket
	queues      map[string]int
	status      int32
	tcpListener []net.Listener
	msgRouter   chan *msgRouter
	register    chan *register   //register session
	unregister  chan *unregister //unregister session
	connProxys  map[string]*ConnProxy
	config      *config
	//hooks
	onAccept    onAccept
	onClose     onClose
	onConnect   onConnect
	onSubscribe onSubscribe
	onUnsubscribed  onUnsubscribed
	onPublish   onPublish
	onStop      onStop
	subscriptionInfo  *subscriptionInfo
	serverMonitor    *ServerMonitor
}


//客户端连接注册
type register struct {
	connProxy  *ConnProxy
	connectPacket *packets.ConnectPacket
	error   error
}

//客户端
type unregister struct {
	connProxy  *ConnProxy
	done chan struct{}
}

type msgRouter struct {
	forceBroadcast bool
	connProxyIDs      map[string]struct{} //key by connProxy IDs
	pub            *packets.PublishPacket
}

//定义是否可接受
type onAccept func(conn net.Conn) bool

//packetID
type PacketID = uint16

//配置
type config struct {
	deliveryRetryInterval time.Duration
	queueQos0Messages     bool
	maxInflightMessages   int
	maxQueueMessages      int
}
//关闭方法
type onClose func(connProxy *ConnProxy, err error)


// OnStop will be called on server.Stop()
type onStop func()

type onConnect func(connProxy *ConnProxy) (code uint8)
/*
OnSubscribe 返回topic允许订阅的最高QoS等级
OnSubscribe returns the maximum available QoS for the topic:
 0x00 - Success - Maximum QoS 0
 0x01 - Success - Maximum QoS 1
 0x02 - Success - Maximum QoS 2
 0x80 - Failure
*/
type onSubscribe func(connProxy *ConnProxy, topic string) uint8

// OnUnsubscribed will be called after the topic has been unsubscribed
type onUnsubscribed func(connProxy *ConnProxy, topicName string)

// OnPublish 返回接收到的publish报文是否允许转发，返回false则该报文不会被继续转发
//
// OnPublish returns whether the publish packet will be delivered or not.
// If returns false, the packet will not be delivered to any clients.
type onPublish func(connProxy *ConnProxy, publish *packets.PublishPacket) bool


type subscriptionInfo struct {
	sync.RWMutex
	topicsByConnID   map[string]string //connProxyId 定位到 topicName
	topicsByName map[string]string //topicName 定位到 connProxyId
}
