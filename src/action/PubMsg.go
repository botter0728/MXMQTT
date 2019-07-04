package action

import (
	"github.com/eclipse/paho.mqtt.golang/packets"
	"github.com/wonderivan/logger"
	"go.uber.org/zap"
	"net"
)

/***
* 1、提供发布消息的方法
* 1.1 发布消息 Publish
* 1.2 发布消息确认 Publish Ack
* 1.3 发布消息收到 Publish Rec
* 1.4 发布消息释放 Publish Release
* 1.5 发布消息完成 Publish Comp
*
**/

//发布消息 Publish
func pub(conn net.Conn) {

}

//发布消息确认 Publish Ack
func pubAck(conn net.Conn) {
	//读取连接的数据包
	readPacket, readErr := packets.ReadPacket(conn)

	if readErr != nil {
		logger.Error("[ACTION] Error handler publish ack on ", readErr)
	}

	if readPacket == nil {
		logger.Error("[ACTION] Error handler publish  receive readPacket nil")
		return
	}

	//读取连接类型的数据包
	publishPacket, ok := readPacket.(*packets.PublishPacket)

	if !ok {
		logger.Error("[ACTION] Error receive publish msg that was not right")
		return
	}

	logger.Info("[ACTION] receive message id:  ", zap.String("MessageID", string(publishPacket.MessageID)))

	//连接反馈
	connAck := packets.NewControlPacket(packets.Connack).(*packets.ConnackPacket)
	connAck.ReturnCode = packets.Accepted
	connAck.SessionPresent = connectPacket.CleanSession
	writeErr :=  connAck.Write(conn)

	if writeErr != nil {
		logger.Error("[ACTION] connAck write Error  ", writeErr)
	}
}


//发布消息收到 Publish Rec
func pubRecive(conn net.Conn) {
	//读取连接的数据包
	readPacket, readErr := packets.ReadPacket(conn)

	if readErr != nil {
		logger.Error("[ACTION] Error handlerConn on ", readErr)
	}

	if readPacket == nil {
		logger.Error("[ACTION] Error handlerConn receive readPacket nil")
		return
	}

	//读取连接类型的数据包
	connectPacket, ok := readPacket.(*packets.ConnectPacket)

	if !ok {
		logger.Error("[ACTION] Error receive msg that was not Connect")
		return
	}

	logger.Info("[ACTION] connect from  ", zap.String("clientId", connectPacket.ClientIdentifier))

	//连接反馈
	connAck := packets.NewControlPacket(packets.Connack).(*packets.ConnackPacket)
	connAck.ReturnCode = packets.Accepted
	connAck.SessionPresent = connectPacket.CleanSession
	writeErr :=  connAck.Write(conn)

	if writeErr != nil {
		logger.Error("[ACTION] connAck write Error  ", writeErr)
	}
}

//发布消息释放 Publish Release
func pubRelease(conn net.Conn) {
	//读取连接的数据包
	readPacket, readErr := packets.ReadPacket(conn)

	if readErr != nil {
		logger.Error("[ACTION] Error handlerConn on ", readErr)
	}

	if readPacket == nil {
		logger.Error("[ACTION] Error handlerConn receive readPacket nil")
		return
	}

	//读取连接类型的数据包
	connectPacket, ok := readPacket.(*packets.ConnectPacket)

	if !ok {
		logger.Error("[ACTION] Error receive msg that was not Connect")
		return
	}

	logger.Info("[ACTION] connect from  ", zap.String("clientId", connectPacket.ClientIdentifier))

	//连接反馈
	connAck := packets.NewControlPacket(packets.Connack).(*packets.ConnackPacket)
	connAck.ReturnCode = packets.Accepted
	connAck.SessionPresent = connectPacket.CleanSession
	writeErr :=  connAck.Write(conn)

	if writeErr != nil {
		logger.Error("[ACTION] connAck write Error  ", writeErr)
	}
}

//发布消息完成 Publish Complete
func pubComplete(conn net.Conn) {
	//读取连接的数据包
	readPacket, readErr := packets.ReadPacket(conn)

	if readErr != nil {
		logger.Error("[ACTION] Error handlerConn on ", readErr)
	}

	if readPacket == nil {
		logger.Error("[ACTION] Error handlerConn receive readPacket nil")
		return
	}

	//读取连接类型的数据包
	connectPacket, ok := readPacket.(*packets.ConnectPacket)

	if !ok {
		logger.Error("[ACTION] Error receive msg that was not Connect")
		return
	}

	logger.Info("[ACTION] connect from  ", zap.String("clientId", connectPacket.ClientIdentifier))

	//连接反馈
	connAck := packets.NewControlPacket(packets.Connack).(*packets.ConnackPacket)
	connAck.ReturnCode = packets.Accepted
	connAck.SessionPresent = connectPacket.CleanSession
	writeErr :=  connAck.Write(conn)

	if writeErr != nil {
		logger.Error("[ACTION] connAck write Error  ", writeErr)
	}
}