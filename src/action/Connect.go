package action

import (
	"github.com/eclipse/paho.mqtt.golang/packets"
	"github.com/wonderivan/logger"
	"go.uber.org/zap"
	"net"
)

/****
* 1. 设备连接
* 2. 设备连接确认
* 3. 设备断开连接
* 4. 设备Ping
* 5. 设备Ping ACK
***/

/***
* 设备连接
* 1. 客户端连接报文，相同重复第二次连接报文作为违规
* 2. 校验客户端的唯一表示，will主题，will消息，用户和密码
* 3. cleanSession:0 连接断开后，重新消费QoS1 QoS2 QoS0的消息
* 4. cleanSession:1 连接断开后，丢弃QoS1 QoS2 QoS0的消息
*/
func conn(hostsport string) {

}

//设备连接确认
func connAck(conn net.Conn) {
	//读取连接的数据包
	readPacket, readErr := packets.ReadPacket(conn)

	if readErr != nil {
		logger.Error("[Connect] Error handlerConn on ", readErr)
	}

	if readPacket == nil {
		logger.Error("[Connect] Error handlerConn receive readPacket nil")
		return
	}

	//读取连接类型的数据包
	connectPacket, ok := readPacket.(*packets.ConnectPacket)

	if !ok {
		logger.Error("[Connect] Error receive msg that was not Connect")
		return
	}

	logger.Info("[Connect] connect from  ", zap.String("clientId", connectPacket.ClientIdentifier))

	//连接反馈
	connAck := packets.NewControlPacket(packets.Connack).(*packets.ConnackPacket)
	connAck.ReturnCode = packets.Accepted
	connAck.SessionPresent = connectPacket.CleanSession
	writeErr :=  connAck.Write(conn)

	if writeErr != nil {
		logger.Error("[Connect] connAck write Error  ", writeErr)
	}

}

//设备断开连接
func disConn() {

}

//设备Ping
func ping(conn net.Conn) {
	//读取连接的数据包
	readPacket, readErr := packets.ReadPacket(conn)

	if readErr != nil {
		logger.Error("[Connect] Error handler pingResp on ", readErr)
	}

	if readPacket == nil {
		logger.Error("[Connect] Error handler pingResp receive readPacket nil")
		return
	}

	//读取PING类型的数据包
	pingReqPacket, ok := readPacket.(*packets.PingreqPacket)

	if !ok {
		logger.Error("[Connect] Error receive ping msg that was fail")
		return
	}

	logger.Info("[Connect] ping data:  ", zap.String("pingReqPacket", pingReqPacket.String()))

	//连接反馈
	pingResp := packets.NewControlPacket(packets.Pingresp).(*packets.PingrespPacket)
	writeErr := pingResp.Write(conn)
	if writeErr != nil {
		logger.Error("[Connect] pingResp write Error  ", writeErr)
	}
}

//设备Ping ACK
func pingResp(conn net.Conn) {

}