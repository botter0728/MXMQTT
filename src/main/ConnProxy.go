package main

import (
	"bufio"
	"container/list"
	"crypto/md5"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/eclipse/paho.mqtt.golang/packets"
	"github.com/wonderivan/logger"
	"io"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"
)



//连接后，需要处理每个连接
type ConnProxy struct {
	mxmqtt        *MXMQTT
	waitGroup     sync.WaitGroup
	conn          net.Conn
	bufferReader  *bufio.Reader
	bufferWriter  *bufio.Writer
	packetReader  *PacketReader
	packetWriter  *PacketWriter
	dataWriter    chan packets.ControlPacket
	dataReader    chan packets.ControlPacket
	writeMutex    sync.Mutex
	close         chan struct{} //关闭chan
	closeComplete chan struct{} //连接关闭
	status        int32         //connProxy状态
	session       *ConnSession
	error         chan error //错误
	err           error
	opts          *ConnProxyOptions //OnConnect之前填充,set up before OnConnect()
	cleanWillFlag bool              //收到DISCONNECT报文删除遗嘱标志, whether to remove will msg
	//自定义数据 user data
	userMutex sync.Mutex
	userData  interface{}
	ready chan struct{} //close after session prepared
}

/**
* 连接操作
 */
type ConnProxyOptions struct {
	connProxyID  string
	Username     string
	Password     string
	KeepAlive    uint16
	CleanSession bool
	WillFlag     bool
	WillRetain   bool
	WillQos      uint8
	WillTopic    string
	WillPayload  []byte
}

/**
* 定义数据包读缓冲区
 */
type PacketReader struct {
	bufferRead *bufio.Reader
}

/**
* 定义数据包写缓冲区
 */
type PacketWriter struct {
	bufferWrite *bufio.Writer
}


/***
* 定义读和写缓冲区大小
 */
const (
	readBufferSize  = 8096
	writeBufferSize = 8096
	redeliveryTime  = 10
)

// Error
var (
	ErrInvalStatus    = errors.New("invalid connection status")
	ErrConnectTimeOut = errors.New("connect time out")
)

var (
	ErrInvalPacketType           = errors.New("invalid Packet Type")
	ErrInvalFlags                = errors.New("invalid Flags")
	ErrInvalConnFlags            = errors.New("invalid Connect Flags")
	ErrInvalConnAcknowledgeFlags = errors.New("invalid Connect Acknowledge Flags")
	ErrInvalSessionPresent       = errors.New("invalid Session Present")
	ErrInvalRemainLength         = errors.New("Malformed Remaining Length")
	ErrInvalProtocolName         = errors.New("invalid protocol name")
	ErrInvalUtf8                 = errors.New("invalid utf-8 string")
	ErrInvalTopicName            = errors.New("invalid topic name")
	ErrInvalTopicFilter          = errors.New("invalid topic filter")
	ErrInvalQos                  = errors.New("invalid Qos,only support qos0 | qos1 | qos2")
	ErrInvalWillQos              = errors.New("invalid Will Qos")
	ErrInvalWillRetain           = errors.New("invalid Will Retain")
	ErrInvalUTF8String           = errors.New("invalid utf-8 string")
)


/***
* 定义连接状态
 */
const (
	Connecting = iota
	Connected
	Switiching
	Disconnected
)

/**
* 定义异步池子
 */
var (
	bufioReaderPool sync.Pool
	bufioWriterPool sync.Pool
)
var pid = os.Getpid()
var counter uint32
var machineId = readMachineId()


const (
	QOS_0             = 0x00
	QOS_1             = 0x01
	QOS_2             = 0x02
	SUBSCRIBE_FAILURE = 0x80
)

/**
* 读取数据
 */
func (packetRead *PacketReader) readPacket() (packets.ControlPacket, error) {
	first, err := packetRead.bufferRead.ReadByte()

	if err != nil {
		return nil, err
	}

	fixedHeader := &packets.FixedHeader{MessageType: first >> 4, Qos: first & 15} //设置FixHeader
	length, err := packets.DecodeLength(packetRead.bufferRead)
	if err != nil {
		return nil, err
	}
	fixedHeader.RemainingLength = length
	packet, err := packets.ReadPacket(packetRead.bufferRead)
	return packet, err
}

//写数据
func (packetWriter *PacketWriter) WritePacket(packet packets.ControlPacket) error {
	err := packet.Write(packetWriter.bufferWrite)
	if err != nil {
		return err
	}
	return nil
}

//写完flush
func (packetWriter *PacketWriter) WriteAndFlush(packet packets.ControlPacket) error {
	err := packet.Write(packetWriter.bufferWrite)
	if err != nil {
		return err
	}
	return packetWriter.bufferWrite.Flush()
}

/**
* 创建连接代理Proxy
 */
func (mxmqtt *MXMQTT) newConnProxy(conn net.Conn) *ConnProxy {
	connProxy := &ConnProxy{
		mxmqtt:        mxmqtt,
		conn:          conn,
		bufferReader:  newBufferIOReaderSize(conn, readBufferSize),
		bufferWriter:  newBufferIOWriterSize(conn, writeBufferSize),
		close:         make(chan struct{}),
		closeComplete: make(chan struct{}),
		error:         make(chan error, 1),
		dataWriter:    make(chan packets.ControlPacket, readBufferSize),
		dataReader:    make(chan packets.ControlPacket, writeBufferSize),
		status:        Connecting,
		opts:          &ConnProxyOptions{},
		cleanWillFlag: false,
		ready:         make(chan struct{}),
	}

	connProxy.packetReader = newPacketReader(connProxy.bufferReader)
	connProxy.packetWriter = newPacketWriter(connProxy.bufferWriter)
	connProxy.setConnecting()
	connProxy.newConnSession()
	return connProxy
}

//获取所有的connProxyOptions
func (connProxy *ConnProxy) connProxyOptions() ConnProxyOptions {
	opts := *connProxy.opts
	opts.WillPayload = make([]byte, len(connProxy.opts.WillPayload))
	copy(opts.WillPayload, connProxy.opts.WillPayload)
	return opts
}

/**
* 获取读取文件的大小
 */
func newBufferIOReaderSize(read io.Reader, size int) *bufio.Reader {
	if v := bufioReaderPool.Get(); v != nil {
		bufferReader := v.(*bufio.Reader)
		bufferReader.Reset(read)
		return bufferReader
	}
	return bufio.NewReaderSize(read, size)
}
/***
* 获取写入文件的大小
 */
func newBufferIOWriterSize(write io.Writer, size int) *bufio.Writer {
	if v := bufioWriterPool.Get(); v != nil {
		bufferWrite := v.(*bufio.Writer)
		bufferWrite.Reset(write)
		return bufferWrite
	}
	return bufio.NewWriterSize(write, size)
}

//按照read数据，转化成PacketReader
func newPacketReader(read io.Reader) *PacketReader {
	if bufferRead, ok := read.(*bufio.Reader); ok {
		return &PacketReader{bufferRead: bufferRead}
	}
	return &PacketReader{bufferRead: bufio.NewReaderSize(read, 2048)}
}

//将写入的数据转化成PacketWriter
func newPacketWriter(write io.Writer) *PacketWriter {
	if bufw, ok := write.(*bufio.Writer); ok {
		return &PacketWriter{bufferWrite: bufw}
	}
	return &PacketWriter{bufferWrite: bufio.NewWriterSize(write, 2048)}
}

//read数据从buffer
func putBufferIOReader(bufferRead *bufio.Reader) {
	bufferRead.Reset(nil)
	bufioReaderPool.Put(bufferRead)
}

//buffer 写入数据
func putBufferIOWriter(bufferWrite *bufio.Writer) {
	bufferWrite.Reset(nil)
	bufioWriterPool.Put(bufferWrite)
}

//设置正在连接
func (connProxy *ConnProxy) setConnecting() {
	atomic.StoreInt32(&connProxy.status, Connecting)
}
//设置交换
func (connProxy *ConnProxy) setSwitching() {
	atomic.StoreInt32(&connProxy.status, Switiching)
}

//设置已经连接
func (connProxy *ConnProxy) setConnected() {
	atomic.StoreInt32(&connProxy.status, Connected)
}

//设置断开
func (connProxy *ConnProxy) setDisConnected() {
	atomic.StoreInt32(&connProxy.status, Disconnected)
}
//设置错误
func (connProxy *ConnProxy) setError(error error) {
	select {
	case connProxy.error <- error:
	default:
	}
}
//是否连接
func (connProxy *ConnProxy) isConnected() bool {
	return connProxy.getStatus() == Connected
}
/**
* 获取状态
 */
func (connProxy *ConnProxy) getStatus() int32 {
	return atomic.LoadInt32(&connProxy.status)
}


/***
* 创建连接Session
 */
func (connProxy *ConnProxy) newConnSession() {
	connSession := &ConnSession{
		unackpublish:        make(map[PacketID]bool),
		inflight:            list.New(),
		msgQueue:            list.New(),
		lockedPid:           make(map[PacketID]bool),
		freePid:             1,
		maxInflightMessages: connProxy.mxmqtt.config.maxInflightMessages,
		maxQueueMessages:    connProxy.mxmqtt.config.maxQueueMessages,
	}
	connProxy.session = connSession
}

//关闭
func (connProxy *ConnProxy) internalClose() {
	defer close(connProxy.closeComplete)
	if connProxy.getStatus() != Switiching {
		unregister := &unregister{connProxy: connProxy, done: make(chan struct{})}
		connProxy.mxmqtt.unregister <- unregister
		<-unregister.done
	}
	//读数据到readbuffer Pool中
	putBufferIOReader(connProxy.bufferReader)
	//写数据到writebuffer Pool中
	putBufferIOWriter(connProxy.bufferWriter)

	//关闭方法
	if connProxy.mxmqtt.onClose != nil {
		connProxy.mxmqtt.onClose(connProxy, connProxy.err)
	}
}


//server goroutine结束的条件:1客户端断开连接 或 2发生错误
func (connProxy *ConnProxy) runServe() {
	//关闭
	defer connProxy.internalClose()
	//等待3秒
	connProxy.waitGroup.Add(3)
	//监控错误
	go connProxy.errorWatch()
	//read packet
	go connProxy.readDataPacketLoop()
	//write packet
	go connProxy.writeDataPacketLoop()
	//连接超时, 链接成功,建立session
	if ok := connProxy.connectWithTimeOut(); ok {
		//等待2秒
		connProxy.waitGroup.Add(2)
		//连接read数据处理
		go connProxy.readPacketHandle()
		//重新处理数据包方法
		go connProxy.reDeliver()
	}

	connProxy.waitGroup.Wait()
}

/**
* 监听错误,关闭连接
 */
func (connProxy *ConnProxy) errorWatch() {
	defer func() {
		connProxy.waitGroup.Done()
	}()

	select {
	case <-connProxy.close:
		return
	case err := <-connProxy.error: //有错误关闭
		connProxy.err = err
		connProxy.conn.Close()
		close(connProxy.close) //退出chanel
		return
	}
}
/***
* 循环读取数据包
 */
func (connProxy *ConnProxy) readDataPacketLoop() {
	var err error
	defer func() {
		if re := recover(); re != nil {
			err = errors.New(fmt.Sprint(re))
		}
		connProxy.setError(err)
		connProxy.waitGroup.Done()
	}()

	for {
		var packet packets.ControlPacket
		if connProxy.isConnected() {
			if keepAlive := connProxy.opts.KeepAlive; keepAlive != 0 { //KeepAlive
				connProxy.conn.SetReadDeadline(time.Now().Add(time.Duration(keepAlive/2+keepAlive) * time.Second))
			}
		}
		packet, err = connProxy.packetReader.readPacket()
		if err != nil {
			return
		}

		connProxy.dataWriter <- packet
	}
}

/***
* 写入数据包
 */
func (connProxy *ConnProxy) writeDataPacketLoop() {
	var err error
	defer func() {
		if recover := recover(); recover != nil {
			err = errors.New(fmt.Sprint(recover))
		}
		connProxy.setError(err)
		connProxy.waitGroup.Done()
	}()
	for {
		select {
		//关闭
		case <-connProxy.close:
			return
		case packet := <-connProxy.dataReader:
			err = connProxy.writePacket(packet)
			if err != nil {
				return
			}
		}
	}
}

/***
*
*写入数据包
*/
func (connProxy *ConnProxy) writePacket(packet packets.ControlPacket) error {

	logger.Info("%-15s %v: %s ", "sending to", connProxy.conn.RemoteAddr(), packet)

	err := connProxy.writePacket(packet)
	if err != nil {
		return err
	}

	return connProxy.packetWriter.bufferWrite.Flush()
}

/**
* 连接超时处理
 */

func (connProxy *ConnProxy) connectWithTimeOut() (ok bool) {
	var err error
	defer func() {
		if err != nil {
			connProxy.setError(err)
			ok = false
		} else {
			ok = true
		}
	}()
	var p packets.ControlPacket
	select {
	case <-connProxy.close:
		return
	case p = <-connProxy.dataWriter: //first packet
	case <-time.After(5 * time.Second):
		err = ErrConnectTimeOut
		return
	}
	conn, flag := p.(*packets.ConnectPacket)
	if !flag {
		err = ErrInvalStatus
		return
	}
	connProxy.opts.connProxyID = string(conn.ClientIdentifier)
	if connProxy.opts.connProxyID == "" {
		connProxy.opts.connProxyID = getRandomUUID()
	}
	connProxy.opts.KeepAlive = conn.Keepalive
	connProxy.opts.CleanSession = conn.CleanSession
	connProxy.opts.Username = string(conn.Username)
	connProxy.opts.Password = string(conn.Password)
	connProxy.opts.WillFlag = conn.WillFlag
	connProxy.opts.WillPayload = make([]byte, len(conn.WillMessage))
	connProxy.opts.WillQos = conn.WillQos
	connProxy.opts.WillTopic = string(conn.WillTopic)
	copy(connProxy.opts.WillPayload, conn.WillMessage)
	connProxy.opts.WillRetain = conn.WillRetain
	if keepAlive := connProxy.opts.KeepAlive; keepAlive != 0 { //KeepAlive
		connProxy.conn.SetReadDeadline(time.Now().Add(time.Duration(keepAlive/2+keepAlive) * time.Second))
	}
	register := &register{
		connProxy:  connProxy,
		connectPacket: conn,
	}
	select {
	case connProxy.mxmqtt.register <- register:
	case <-connProxy.close:
		return
	}
	select {
	case <-connProxy.close:
		return
	case <-connProxy.ready:
	}
	err = register.error
	return
}

//生成随机码
func getRandomUUID() string {
	var b [12]byte
	// Timestamp, 4 bytes, big endian
	binary.BigEndian.PutUint32(b[:], uint32(time.Now().Unix()))
	// Machine, first 3 bytes of md5(hostname)
	b[4] = machineId[0]
	b[5] = machineId[1]
	b[6] = machineId[2]
	// Pid, 2 bytes, specs don't specify endianness, but we use big endian.
	b[7] = byte(pid >> 8)
	b[8] = byte(pid)
	// Increment, 3 bytes, big endian
	i := atomic.AddUint32(&counter, 1)
	b[9] = byte(i >> 16)
	b[10] = byte(i >> 8)
	b[11] = byte(i)
	return fmt.Sprintf(`%x`, string(b[:]))
}

func readMachineId() []byte {
	id := make([]byte, 3, 3)
	hostname, err1 := os.Hostname()
	if err1 != nil {
		_, err2 := io.ReadFull(rand.Reader, id)
		if err2 != nil {
			panic(fmt.Errorf("cannot get hostname: %v; %v", err1, err2))
		}
		return id
	}
	hw := md5.New()
	hw.Write([]byte(hostname))
	copy(id, hw.Sum(nil))
	return id
}

//读处理
func (connProxy *ConnProxy) readPacketHandle() {
	var err error
	defer func() {
		if re := recover(); re != nil {
			err = errors.New(fmt.Sprint(re))
		}
		connProxy.setError(err)
		connProxy.waitGroup.Done()
	}()

	for {
		select {
		case <-connProxy.close:
			return
		case packet := <-connProxy.dataWriter:
			switch packet.(type) {
			//订阅数据包
			case *packets.SubackPacket:
				connProxy.subscribeHandler(packet.(*packets.SubscribePacket))
				//发布数据包
			case *packets.PublishPacket:
				connProxy.publishHandler(packet.(*packets.PublishPacket))
				//发布反馈数据包
			case *packets.PubackPacket:
				connProxy.pubackHandler(packet.(*packets.PubackPacket))
				//发布数据包释放
			case *packets.PubrelPacket:
				connProxy.pubrelHandler(packet.(*packets.PubrelPacket))
				//发布数据包接受完成
			case *packets.PubrecPacket:
				connProxy.pubrecHandler(packet.(*packets.PubrecPacket))
			case *packets.PubcompPacket:
				connProxy.pubcompHandler(packet.(*packets.PubcompPacket))
			case *packets.PingreqPacket:
				connProxy.pingreqHandler(packet.(*packets.PingreqPacket))
			case *packets.UnsubscribePacket:
				connProxy.unsubscribeHandler(packet.(*packets.UnsubscribePacket))
			case *packets.DisconnectPacket:
				//正常关闭
				connProxy.cleanWillFlag = true
				return
			default:
				err = errors.New("invalid packet")
				return
			}
		}
	}
}

//重传处理
func (connProxy *ConnProxy) reDeliver() {
	var err error
	session := connProxy.session
	defer func() {
		if re := recover(); re != nil {
			err = errors.New(fmt.Sprint(re))
		}
		connProxy.setError(err)
		connProxy.waitGroup.Done()
	}()

	retryInterval := connProxy.mxmqtt.config.deliveryRetryInterval
	timer := time.NewTicker(retryInterval)
	defer timer.Stop()

	for {
		select {
		case <-connProxy.close: //关闭广播
			return
		case <-timer.C: //重发ticker
			session.inflightMu.Lock()
			for inflight := session.inflight.Front(); inflight != nil; inflight = inflight.Next() {
				if inflight, ok := inflight.Value.(*InflightElem); ok {
					if time.Now().Unix()-inflight.time.Unix() >= int64(retryInterval.Seconds()) {
						pub := inflight.publishPacket
						publishPacket := pub.Copy()
						publishPacket.Dup = true
						if inflight.step == 0 {
							connProxy.writePacket(publishPacket)
						}
						if inflight.step == 1 {
							//返回发布消息释放
							pubrel := packets.NewControlPacket(packets.Pubrel)
							connProxy.writePacket(pubrel)
						}
					}
				}
			}
			session.inflightMu.Unlock()
		}
	}
}

/**
* 数据处理handler 开始
*
 */
//订阅消息包处理
func (connProxy *ConnProxy) subscribeHandler(subPacket *packets.SubscribePacket) {
	mxmqtt := connProxy.mxmqtt
	if mxmqtt.onSubscribe != nil {
		for _, v := range subPacket.Topics {
			subPacket.Qos = connProxy.mxmqtt.onSubscribe(connProxy, v)
		}
	}
	//创建新订阅反馈数据包
	subackPacket := packets.NewControlPacket(packets.Suback)
	//发送数据
	connProxy.writePacket(subackPacket)

	mxmqtt.subscriptionInfo.Lock()
	defer mxmqtt.subscriptionInfo.Unlock()

	var isNew bool
	for _, topicName := range subPacket.Topics {
		if subPacket.Qos != packets.Subscribe {

			if mxmqtt.subscribe(connProxy.opts.connProxyID, topicName) {
				isNew = true
			}

			if connProxy.mxmqtt.serverMonitor != nil {
				connProxy.mxmqtt.serverMonitor.subscribe(SubscriptionsInfo{
					connProxyID: connProxy.opts.connProxyID,
					qos: subackPacket.Details().Qos,
					name: topicName,
					time: time.Now(),
				})
			}
		}
	}


	if isNew {
		mxmqtt.retainedMsgMu.Lock()
		for _, msg := range mxmqtt.retainedMsg {
			msg.Retain = true
			msgRouter := &msgRouter{forceBroadcast: false, pub: msg}
			mxmqtt.msgRouter <- msgRouter
		}
		mxmqtt.retainedMsgMu.Unlock()
	}

}

//Publish handler
func (connProxy *ConnProxy) publishHandler(pubPacket *packets.PublishPacket) {
	session := connProxy.session
	mxmqtt := connProxy.mxmqtt
	var dup bool
	if pubPacket.Qos == QOS_1 {
		puback := packets.NewControlPacket(packets.Puback)
		connProxy.writePacket(puback)
	}
	if pubPacket.Qos == QOS_2 {
		pubrec := packets.NewControlPacket(packets.Pubrec)
		connProxy.writePacket(pubrec)
		if _, ok := session.unackpublish[pubPacket.MessageID]; ok {
			dup = true
		} else {
			session.unackpublish[pubPacket.MessageID] = true
		}
	}
	if pubPacket.Retain {
		//保留消息，处理保留
		mxmqtt.retainedMsgMu.Lock()
		mxmqtt.retainedMsg[string(pubPacket.TopicName)] = pubPacket
		if len(pubPacket.Payload) == 0 {
			delete(mxmqtt.retainedMsg, string(pubPacket.TopicName))
		}
		mxmqtt.retainedMsgMu.Unlock()
	}
	if !dup {
		var valid bool
		valid = true
		if mxmqtt.onPublish != nil {
			valid = connProxy.mxmqtt.onPublish(connProxy, pubPacket)
		}
		if valid {
			pubPacket.Retain = false
			msgRouter := &msgRouter{forceBroadcast: false, pub: pubPacket}
			select {
			case <-connProxy.close:
				return
			case connProxy.mxmqtt.msgRouter <- msgRouter:
			}
		}
	}
}
//发布ack handler处理
func (connProxy *ConnProxy) pubackHandler(puback *packets.PubackPacket) {
	connProxy.unsetInflight(puback)
}

//发布释放handler
func (connProxy *ConnProxy) pubrelHandler(pubrel *packets.PubrelPacket) {
	delete(connProxy.session.unackpublish, pubrel.MessageID)
	pubcomp := packets.NewControlPacket(packets.Pubcomp)
	connProxy.writePacket(pubcomp)
}

//使用
func (connProxy *ConnProxy) pubrecHandler(pubrec *packets.PubrecPacket) {
	connProxy.unsetInflight(pubrec)
	pubrel := packets.NewControlPacket(packets.Pubrel)
	connProxy.writePacket(pubrel)
}

//发布完成处理handler
func (connProxy *ConnProxy) pubcompHandler(pubcomp *packets.PubcompPacket) {
	connProxy.unsetInflight(pubcomp)
}
//处理ping请求handler
func (connProxy *ConnProxy) pingreqHandler(pingreq *packets.PingreqPacket) {
	resp := packets.NewControlPacket(packets.Pingresp)
	connProxy.writePacket(resp)
}

//处理未订阅handler
func (connProxy *ConnProxy) unsubscribeHandler(unSub *packets.UnsubscribePacket) {
	mxmqtt := connProxy.mxmqtt
	unSuback := packets.NewControlPacket(packets.Unsuback)
	connProxy.writePacket(unSuback)

	mxmqtt.subscriptionInfo.Lock()
	defer mxmqtt.subscriptionInfo.Unlock()

	for _, topicName := range unSub.Topics {
		mxmqtt.unsubscribe(connProxy.opts.connProxyID, topicName)
		if connProxy.mxmqtt.serverMonitor != nil {
			connProxy.mxmqtt.serverMonitor.unSubscribe(connProxy.opts.connProxyID, topicName)
		}
		if mxmqtt.onUnsubscribed != nil {
			connProxy.mxmqtt.onUnsubscribed(connProxy, topicName)
		}
	}
}
//unsetInflight 出队
//packet: puback(QOS1),pubrec(QOS2)  or pubcomp(QOS2)
func (connProxy *ConnProxy) unsetInflight(packet packets.ControlPacket) {
	session := connProxy.session
	session.inflightMu.Lock()
	defer session.inflightMu.Unlock()
	var freeID bool
	var packetId uint16
	var isRemove bool
	for e := session.inflight.Front(); e != nil; e = e.Next() {
		if el, ok := e.Value.(*InflightElem); ok {
			switch packet.(type) {
			case *packets.PubackPacket: //QOS1
				if el.publishPacket.Qos != QOS_1 {
					continue
				}
				packetId = packet.(*packets.PubackPacket).MessageID
				freeID = true
				isRemove = true
			case *packets.PubrecPacket: //QOS2
				if el.publishPacket.Qos != QOS_2 && el.step != 0 {
					continue
				}
				packetId = packet.(*packets.PubrecPacket).MessageID
			case *packets.PubcompPacket: //QOS2
				if el.publishPacket.Qos != QOS_2 && el.step != 1 {
					continue
				}

				freeID = true //[MQTT-4.3.3-1]. 一旦发送者收到PUBCOMP报文，这个报文标识符就可以重用。
				isRemove = true
				packetId = packet.(*packets.PubcompPacket).MessageID
			}


			if packetId == el.packetId {
				if isRemove {
					session.inflight.Remove(e)
					publish := connProxy.msgDequeue()
					if publish != nil {
						elem := &InflightElem{
							time:     time.Now(),
							packetId:    publish.MessageID,
							publishPacket: publish,
							step:   0,
						}
						session.inflight.PushBack(elem)
						connProxy.dataWriter <- publish
					} else if connProxy.mxmqtt.serverMonitor != nil {
						connProxy.mxmqtt.serverMonitor.delInflight(connProxy.opts.connProxyID)

					}
				} else {
					el.step++
				}
				if freeID {
					session.freePacketID(packetId)
				}
				return
			}
		}
	}

}

/**
* 数据处理handler 结束
*
 */

func (connProxy *ConnProxy) msgDequeue() *packets.PublishPacket {
	s := connProxy.session
	s.msgQueueMu.Lock()
	defer s.msgQueueMu.Unlock()

	if s.msgQueue.Len() > 0 {
		queueElem := s.msgQueue.Front()
		s.msgQueue.Remove(queueElem)
		if connProxy.mxmqtt.serverMonitor != nil {
			connProxy.mxmqtt.serverMonitor.msgDeQueue(connProxy.opts.connProxyID)
		}
		return queueElem.Value.(*packets.PublishPacket)
	}
	return nil
}

