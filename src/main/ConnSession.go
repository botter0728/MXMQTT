package main

import (
	"container/list"
	"fmt"
	"github.com/eclipse/paho.mqtt.golang/packets"
	"sync"
	"time"
)

/**
*
* 处理连接消息出队和入队, 连接session
 */
type ConnSession struct {
	inflightMu sync.Mutex //gard inflight
	inflight   *list.List //传输中等待确认的报文
	msgQueueMu sync.Mutex //gard msgQueue
	msgQueue   *list.List //缓存数据，缓存publish报文
	unackpublish map[PacketID]bool //[MQTT-4.3.3-2]
	pidMu        sync.RWMutex              //gard lockedPid & freeID
	lockedPid    map[PacketID]bool //Pid inuse
	freePid      PacketID          //下一个可以使用的freeID
	maxInflightMessages int
	maxQueueMessages    int
}

type InflightElem struct {
	//At is the entry time
	time time.Time
	//Pid is the packetID
	packetId uint16
	//Packet represents Publish packet
	publishPacket *packets.PublishPacket
	step   int
}

func (session *ConnSession) freePacketID(packetId uint16) {
	session.pidMu.Lock()
	defer session.pidMu.Unlock()
	session.lockedPid[packetId] = false
}

/***
* 消息入队
 */
func (connProxy *ConnProxy) messageEnQueue(publishPacket *packets.PublishPacket) {
	connSession := connProxy.session

	connSession.msgQueueMu.Lock()
	defer connSession.msgQueueMu.Unlock()

	if connSession.msgQueue.Len() >= connSession.maxQueueMessages && connSession.maxQueueMessages != 0 {
		if connProxy.mxmqtt.serverMonitor != nil {
			connProxy.mxmqtt.serverMonitor.msgQueueDropped(connProxy.opts.connProxyID)
		}

		fmt.Printf("%-15s[%s]", "msg queue is overflow, removing msg. ", connProxy.connProxyOptions().connProxyID)

		var removeMsg *list.Element
		for front := connSession.msgQueue.Front(); front != nil; front = front.Next() {
			if publishPacket, ok := front.Value.(*packets.PublishPacket); ok {
				if publishPacket.Qos == QOS_0 {
					removeMsg = front
					break
				}
			}
		}

		if removeMsg != nil {
			connSession.msgQueue.Remove(removeMsg)

			fmt.Printf("%-15s[%s],packet: %s ", "qos 0 msg removed", connProxy.connProxyOptions().connProxyID, removeMsg.Value.(packets.ControlPacket))

		} else if publishPacket.Qos == QOS_0 {
			return
		} else {
			front := connSession.msgQueue.Front()
			connSession.msgQueue.Remove(front)

			p := front.Value.(packets.ControlPacket)
			fmt.Printf("%-15s[%s],packet: %s ", "first msg removed", connProxy.connProxyOptions().connProxyID, p)

		}
	} else if connProxy.mxmqtt.serverMonitor != nil {
		connProxy.mxmqtt.serverMonitor.msgEnQueue(connProxy.opts.connProxyID)
	}

	connSession.msgQueue.PushBack(publishPacket)

}

/***
*
* 消息出队
*/
func (connProxy *ConnProxy) messageDequeue() *packets.PublishPacket {
	connSession := connProxy.session
	connSession.msgQueueMu.Lock()
	defer connSession.msgQueueMu.Unlock()

	if connSession.msgQueue.Len() > 0 {
		queueElem := connSession.msgQueue.Front()

		fmt.Printf("%-15s[%s],packet: %s ", "sending queued msg ", connProxy.connProxyOptions().connProxyID, queueElem.Value.(*packets.PublishPacket))

		connSession.msgQueue.Remove(queueElem)
		if connProxy.mxmqtt.serverMonitor != nil {
			connProxy.mxmqtt.serverMonitor.msgDeQueue(connProxy.opts.connProxyID)
		}

		return queueElem.Value.(*packets.PublishPacket)
	}
	return nil
}