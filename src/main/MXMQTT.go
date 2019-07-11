package main

import (
	"errors"
	"fmt"
	"github.com/eclipse/paho.mqtt.golang/packets"
	"github.com/wonderivan/logger"
	"go.uber.org/zap"
	"log"
	"net"
	"strconv"
)

/***
*
*基于go编写的MQTT BROKER
* 满足：
* 1. 提供基于tcp 1883 连接
* 2. 提供发布和订阅, 消息订阅反馈。
* 3. 提供websokcet连接
* 4. 提供http(s)连接
*
 */

// Server status
const (
	statusInit = iota
	statusStarted
)

/***
*
* mxmqtt 启动方法，
 */
func (mxmqtt *MXMQTT) start() {
	//设置启动状态
	mxmqtt.status = statusStarted

	//监控事件轮询
	go mxmqtt.eventLoop()

	//
	for _, tcpListener := range mxmqtt.tcpListener {
		go mxmqtt.serverTCP(tcpListener)
	}

}

/***
*
* broker 轮询轮询事件，接受体是MXMQTT,是mxmqtt的一个定义函数
 */
func (mxmqtt *MXMQTT) eventLoop() {
	for {
		select {
		case register := <-mxmqtt.register:
			mxmqtt.onConnHandler(register)
		case unregister := <-mxmqtt.unregister:
			mxmqtt.onDisConnHandler(unregister)
		case msg := <-mxmqtt.msgRouter:
			mxmqtt.msgRouterHandler(msg)
		}
	}
}

/***
*
* 启动监听端口,TCP端口
*
 */
func (mxmqtt *MXMQTT) serverTCP(tcpListener net.Listener) {
	//保证最后线程也要关闭
	defer func() { tcpListener.Close() }()

	//监控消息
	for {
		conn, er := tcpListener.Accept()
		//异常处理
		if er != nil {
			return
		}
		//正常接受消息
		if conn != nil {
			if !mxmqtt.onAccept(conn) {
				//不能接受消息，关闭此通道
				conn.Close()
				continue
			}
		}

		connProxy := mxmqtt.newConnProxy(conn)
		//处理消息
		go connProxy.runServe()
	}
}

/***
* 连接处理
 */
func (mxmqtt *MXMQTT) onConnHandler(register *register) {
	connProxy := register.connProxy
	defer close(connProxy.ready)

	connectPacket := register.connectPacket
	var sessionReuse bool
	if connectPacket.Keepalive == packets.Accepted {
		err := errors.New("reject connection, ack code:" + strconv.Itoa(int(connectPacket.Keepalive)))
		ack := packets.NewControlPacket(packets.Connack)
		//client.out <- ack
		connProxy.writePacket(ack)
		register.error = err
		return
	}

	if mxmqtt.onConnect != nil {
		code := mxmqtt.onConnect(connProxy)
		connectPacket.Keepalive = uint16(code)
		if code != packets.Connack {
			err := errors.New("reject connection, ack code:" + strconv.Itoa(int(code)))
			ack := packets.NewControlPacket(packets.Connack)
			//client.out <- ack
			connProxy.writePacket(ack)
			connProxy.setError(err)
			register.error = err
			return
		}
	}

	mxmqtt.rwMu.Lock()
	defer mxmqtt.rwMu.Unlock()

	var oldSession *ConnSession
	oldClient, oldExist := mxmqtt.connProxys[connProxy.opts.connProxyID]
	mxmqtt.connProxys[connProxy.opts.connProxyID] = connProxy

	if oldExist {
		oldSession = oldClient.session
		if oldClient.status == Connected {

			fmt.Printf("%-15s %v: logging with duplicate ClientID: %s", "", connProxy.conn.RemoteAddr(), connProxy.connProxyOptions().connProxyID)

			oldClient.setSwitching()
			<-oldClient.close
			if oldClient.opts.WillFlag {
				willMsg := &packets.PublishPacket{
					TopicName: oldClient.opts.WillTopic,
					Payload:   oldClient.opts.WillPayload,
				}
				go func() {
					msgRouter := &msgRouter{forceBroadcast: false, pub: willMsg}
					mxmqtt.msgRouter <- msgRouter
				}()
			}

			if !connProxy.opts.CleanSession && !oldClient.opts.CleanSession { //reuse old session
				sessionReuse = true
			clearOut:
				for {
					select {
					case publishPacket := <-oldClient.dataWriter:
						if publishPacket, ok := publishPacket.(*packets.PublishPacket); ok {
							oldClient.messageEnQueue(publishPacket)
						}
					default:
						break clearOut
					}
				}
			}
		} else if oldClient.getStatus() == Disconnected {
			if !connProxy.opts.CleanSession {
				sessionReuse = true
			}
		}
	}
	//生成连接ACK包
	ack := packets.NewControlPacket(packets.Connack)
	connProxy.dataWriter <- ack
	connProxy.setConnected()

	if sessionReuse { //发送还未确认的消息和离线消息队列 inflight & msgQueue
		connProxy.session.maxInflightMessages = oldSession.maxInflightMessages
		connProxy.session.maxQueueMessages = oldSession.maxQueueMessages
		connProxy.session.unackpublish = oldSession.unackpublish
		oldSession.inflightMu.Lock()
		for e := oldSession.inflight.Front(); e != nil; e = e.Next() { //write unacknowledged publish & pubrel
			if inflight, ok := e.Value.(*InflightElem); ok {
				pub := inflight.publishPacket
				pub.Dup = true
				if inflight.step == 0 {
					connProxy.publishHandler(pub)
				}
				if inflight.step == 1 { //pubrel
					pubrel := packets.NewControlPacket(packets.Pubrel)
					connProxy.session.inflight.PushBack(inflight)
					connProxy.session.freePacketID(pub.MessageID)
					connProxy.dataWriter <- pubrel
				}
			}
		}

		oldSession.inflightMu.Unlock()
		oldSession.msgQueueMu.Lock()
		for front := oldSession.msgQueue.Front(); front != nil; front = front.Next() { //write offline msg
			if publish, ok := front.Value.(*packets.PublishPacket); ok {
				connProxy.publishHandler(publish)
			}
		}

		oldSession.msgQueueMu.Unlock()
		fmt.Printf("%-15s %v: logined with session reuse", "", connProxy.conn.RemoteAddr())

	} else {
		if oldExist {
			mxmqtt.subscriptionInfo.Lock()
			mxmqtt.removeClientSubscriptions(connProxy.opts.connProxyID)
			mxmqtt.subscriptionInfo.Unlock()
		}

		fmt.Printf("%-15s %v: logined with new session", "", connProxy.conn.RemoteAddr())

	}
	if mxmqtt.serverMonitor != nil {
		mxmqtt.serverMonitor.register(connProxy, sessionReuse)
	}
}

/***
*
* 断开连接
 */
func (mxmqtt *MXMQTT) onDisConnHandler(unregister *unregister) {
	defer close(unregister.done)
	connProxy := unregister.connProxy
	connProxy.setDisConnected()
	if connProxy.session == nil {
		return
	}
clearIn:
	for {
		select {
		case p := <-connProxy.dataReader:
			if _, ok := p.(*packets.DisconnectPacket); ok {
				connProxy.cleanWillFlag = true
			}
		default:
			break clearIn
		}
	}

	if !connProxy.cleanWillFlag && connProxy.opts.WillFlag {
		//发布遗言消息
		willMsg := &packets.PublishPacket{
			FixedHeader: packets.FixedHeader{MessageType: packets.Publish, Dup: false, Qos: connProxy.opts.WillQos, Retain: false},
			TopicName:   connProxy.opts.WillTopic,
			Payload:     connProxy.opts.WillPayload,
		}

		go func() {
			msgRouter := &msgRouter{forceBroadcast: false, pub: willMsg}
			connProxy.mxmqtt.msgRouter <- msgRouter
		}()
	}

	if connProxy.opts.CleanSession {

		log.Printf("%-15s %v: logout & cleaning session", "", connProxy.conn.RemoteAddr())

		mxmqtt.rwMu.Lock()
		delete(mxmqtt.connProxys, connProxy.opts.connProxyID)
		mxmqtt.subscriptionInfo.Lock()
		mxmqtt.removeClientSubscriptions(connProxy.opts.connProxyID)
		mxmqtt.subscriptionInfo.Unlock()
		mxmqtt.rwMu.Unlock()
	} else {
		//store session 保持session

		log.Printf("%-15s %v: logout & storing session", "", connProxy.conn.RemoteAddr())

		//clear  out
	clearOut:
		for {
			select {
			case p := <-connProxy.dataReader:
				if p, ok := p.(*packets.PublishPacket); ok {
					connProxy.publishHandler(p)
				}
			default:
				break clearOut
			}
		}
	}

	if mxmqtt.serverMonitor != nil {
		mxmqtt.serverMonitor.unRegister(connProxy.opts.connProxyID, connProxy.opts.CleanSession)
	}
}

//消息路由
func (mxmqtt *MXMQTT) msgRouterHandler(msgRouter *msgRouter) {
	mxmqtt.rwMu.RLock()
	defer mxmqtt.rwMu.RUnlock()

	pub := msgRouter.pub
	if msgRouter.forceBroadcast {
		//broadcast
		publishPacket := pub.Copy()
		publishPacket.Dup = false

		if len(msgRouter.connProxyIDs) != 0 {
			for cid := range msgRouter.connProxyIDs {
				if _, ok := mxmqtt.connProxys[cid]; ok {
					mxmqtt.connProxys[cid].publishHandler(publishPacket)
				}
			}
		} else {
			for _, c := range mxmqtt.connProxys {
				c.publishHandler(publishPacket)
			}
		}
		return
	}

	mxmqtt.subscriptionInfo.RLock()
	defer mxmqtt.subscriptionInfo.RUnlock()

	m := make(map[string]uint8)

	connProxyIDslen := len(msgRouter.connProxyIDs)
	for topicName, connProxyId := range mxmqtt.subscriptionInfo.topicsByName {
		if pub.TopicName == topicName { //找到能匹配当前主题订阅等级最高的客户端
			if connProxyIDslen != 0 { //to specific clients
				for cid := range msgRouter.connProxyIDs {
					if qos, ok := m[connProxyId]; ok {
						if pub.Qos > qos {
							m[cid] = pub.Qos
						}
					} else {
						m[cid] = pub.Qos
					}

				}
			} else {
				if qos, ok := m[connProxyId]; ok {
					if pub.Qos > qos {
						m[connProxyId] = pub.Qos
					}
				} else {
					m[connProxyId] = pub.Qos
				}
			}
		}
	}

	for cid, qos := range m {
		publish := pub.Copy()
		if publish.Qos > qos {
			publish.Qos = qos
		}
		publish.Dup = false
		if c, ok := mxmqtt.connProxys[cid]; ok {
			c.publishHandler(publish)
		}
	}
}

/***
*
* 处理连接
 */
func (mxmqtt *MXMQTT) handlerConn(conn net.Conn) {

	//读取连接的数据包
	readPacket, readErr := packets.ReadPacket(conn)

	if readErr != nil {
		logger.Error("[MXMQTT] Error handlerConn on ", readErr)
	}

	if readPacket == nil {
		logger.Error("[MXMQTT] Error handlerConn receive readPacket nil")
		return
	}

	//读取连接类型的数据包
	connectPacket, msgOk := readPacket.(*packets.ConnectPacket)

	if !msgOk {
		logger.Error("[MXMQTT] Error receive msg that was not Connect")
		return
	}

	logger.Info("[MXMQTT] connect from ", zap.String("clientId", connectPacket.ClientIdentifier))

	//连接反馈
	connAck := packets.NewControlPacket(packets.Connack).(*packets.ConnackPacket)
	connAck.ReturnCode = packets.Accepted
	connAck.SessionPresent = connectPacket.CleanSession
	writeErr := connAck.Write(conn)

	if writeErr != nil {
		logger.Error("[MXMQTT] connAck write Error  ", writeErr)
	}
}

//return whether it is a new subscription
func (mxmqtt *MXMQTT) subscribe(connProxyID string, topic string) bool {
	var isNew bool
	mxmqtt.subscriptionInfo.init(connProxyID, topic)
	isNew = !mxmqtt.subscriptionInfo.exist(connProxyID, topic)
	mxmqtt.subscriptionInfo.topicsByConnID[connProxyID] = topic
	mxmqtt.subscriptionInfo.topicsByName[topic] = topic
	return isNew
}

func (mxmqtt *MXMQTT) unsubscribe(clientID string, topicName string) {
	mxmqtt.subscriptionInfo.remove(clientID, topicName)
}

//初始化subscriptionInfo
func (subInfo *subscriptionInfo) init(connProxyID string, topicName string) {
	if _, ok := subInfo.topicsByConnID[connProxyID]; !ok {
		var topicnameTemp string
		subInfo.topicsByConnID[connProxyID] = topicnameTemp
	}
	if _, ok := subInfo.topicsByName[topicName]; !ok {
		var connproxyidTemp string
		subInfo.topicsByName[topicName] = connproxyidTemp
	}
}

// exist returns true if subscription is existed 判断订阅是否存在
func (subInfo *subscriptionInfo) exist(connProxyID string, topicName string) bool {
	if _, ok := subInfo.topicsByName[topicName]; !ok {
		return false
	}
	return true
}

//删除一条记录
func (subInfo *subscriptionInfo) remove(connProxyID string, topicName string) {
	if _, ok := subInfo.topicsByName[topicName]; ok {
		delete(subInfo.topicsByName, connProxyID)
		if len(subInfo.topicsByName[topicName]) == 0 {
			delete(subInfo.topicsByName, topicName)
		}
	}
	if _, ok := subInfo.topicsByConnID[connProxyID]; ok {
		delete(subInfo.topicsByConnID, topicName)
		if len(subInfo.topicsByConnID[connProxyID]) == 0 {
			delete(subInfo.topicsByConnID, connProxyID)
		}
	}
}

//通过connProxyID删除
func (mxmqtt *MXMQTT) removeClientSubscriptions(connProxyID string) {
	subInfo := mxmqtt.subscriptionInfo
	topicName := subInfo.topicsByConnID[connProxyID]
	delete(subInfo.topicsByName, topicName)
	delete(subInfo.topicsByConnID, connProxyID)
}
