package main

import (
	"github.com/botter0728/MXMQTT/mxmqtt/action"
	"github.com/botter0728/MXMQTT/mxmqtt/types"
	"github.com/eclipse/paho.mqtt.golang/packets"
	"github.com/wonderivan/logger"
	"go.uber.org/zap"
	"net"
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
	defer func() {tcpListener.Close()}()

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

		//处理消息
		go mxmqtt.handlerConn(conn)
	}
}

/***
*
* 处理连接
*/
func (mxmqtt *MXMQTT) onConnHandler(register *register) {

}
/***
*
*断开处理
 */
func (mxmqtt *MXMQTT) onDisConnHandler(unregister *unregister) {

}

func (mxmqtt *MXMQTT) msgRouterHandler(router *msgRouter) {

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

func doMessageAction() {

}
