package main

import (
"sort"
"sync"
"time"
)

// connProxyStatus
const (
	StatusOnline  = "online"
	StatusOffline = "offline"
)

// MonitorRepository is an interface which can be used to provide a persistence mechanics for the monitor data
type ServerMonitorRepository interface {
	//打开监控
	open() error
	//关闭
	close() error
	//放入
	putConnProxyInfo(connProxyInfo ConnProxyInfo)
	//根据connProxyID获取客户端信息
	getconnProxyInfo(connProxyID string) (ConnProxyInfo, bool)
	///获取所有客户端信息
	getConnProxyInfos() ConnProxyList
	//根据connProxyID 删除
	delconnProxy(connProxyID string)
	//放入客户端缓存session
	putSession(info SessionInfo)
	//根据connProxyID 获取sessioninfo信息
	getSession(connProxyID string) (SessionInfo, bool)
	//获取所有的连接session信息
	getSessions() SessionList
	//删除session
	delSession(connProxyID string)

	getConnProxySubscriptions(connProxyID string) SubscriptionList

	delConnProxySubscriptions(connProxyID string)

	putSubscription(info SubscriptionsInfo)

	delSubscription(connProxyID string, topicName string)

	getSubscriptions() SubscriptionList
}


type ServerMonitor struct {
	sync.Mutex
	serverMonitorRepository ServerMonitorRepository
}

type ServerMonitorStore struct {
	connProxyInfoMap       map[string]ConnProxyInfo
	sessions      map[string]SessionInfo
	subscriptions map[string]map[string]SubscriptionsInfo //[connProxyID][topicName]
}

func (serverMonitor *ServerMonitor) register(connProxy *ConnProxy, sessionReuse bool) {
	serverMonitor.Lock()
	defer serverMonitor.Unlock()
	connProxyID := connProxy.opts.connProxyID
	username := connProxy.opts.Username
	cinfo := ConnProxyInfo{
		connProxyID:     connProxyID,
		username:     username,
		remoteAddr:   connProxy.conn.RemoteAddr().String(),
		cleanSession: connProxy.opts.CleanSession,
		keepAlive:    connProxy.opts.KeepAlive,
		connectedTime:  time.Now(),
	}
	serverMonitor.serverMonitorRepository.putConnProxyInfo(cinfo)
	if sessionReuse {
		connProxy.session.inflightMu.Lock()
		connProxy.session.msgQueueMu.Lock()
		inflightLen := connProxy.session.inflight.Len()
		msgQueueLen := connProxy.session.msgQueue.Len()
		connProxy.session.inflightMu.Unlock()
		connProxy.session.msgQueueMu.Unlock()
		if c, ok := serverMonitor.serverMonitorRepository.getSession(connProxyID); ok {
			c.connectedTime = time.Now()
			c.status = StatusOnline
			c.inflightLen = inflightLen
			c.msgQueueLen = msgQueueLen
			serverMonitor.serverMonitorRepository.putSession(c)
			return
		}
	}
	serverMonitor.serverMonitorRepository.putSession(SessionInfo{
		connProxyID:        connProxyID,
		status:          StatusOffline,
		remoteAddr:      connProxy.conn.RemoteAddr().String(),
		cleanSession:    connProxy.opts.CleanSession,
		subscriptions:   0,
		maxInflight:     connProxy.session.maxInflightMessages,
		inflightLen:     0,
		maxMsgQueue:     connProxy.session.maxQueueMessages,
		msgQueueDropped: 0,
		msgQueueLen:     0,
		connectedTime:     time.Now(),
	})

}

func (serverMonitor *ServerMonitor) unRegister(connProxyID string, cleanSession bool) {
	serverMonitor.Lock()
	defer serverMonitor.Unlock()
	serverMonitor.serverMonitorRepository.delconnProxy(connProxyID)
	if cleanSession {
		serverMonitor.serverMonitorRepository.delSession(connProxyID)
		serverMonitor.serverMonitorRepository.delConnProxySubscriptions(connProxyID)
	} else {
		if s, ok := serverMonitor.serverMonitorRepository.getSession(connProxyID); ok {
			s.offlineTime = time.Now()
			s.status = StatusOffline
			serverMonitor.serverMonitorRepository.putSession(s)
		}
	}
}


func (serverMonitor *ServerMonitor) subscribe(info SubscriptionsInfo) {
	serverMonitor.Lock()
	defer serverMonitor.Unlock()
	serverMonitor.serverMonitorRepository.putSubscription(info)
	list := serverMonitor.serverMonitorRepository.getConnProxySubscriptions(info.connProxyID)
	if s, ok := serverMonitor.serverMonitorRepository.getSession(info.connProxyID); ok {
		s.subscriptions = len(list)
		serverMonitor.serverMonitorRepository.putSession(s)
	}

}

// unSubscribe deletes the subscription info from repository
func (serverMonitor *ServerMonitor) unSubscribe(connProxyID string, topicName string) {
	serverMonitor.Lock()
	defer serverMonitor.Unlock()
	serverMonitor.serverMonitorRepository.delSubscription(connProxyID, topicName)
	list := serverMonitor.serverMonitorRepository.getConnProxySubscriptions(connProxyID)
	if s, ok := serverMonitor.serverMonitorRepository.getSession(connProxyID); ok {
		s.subscriptions = len(list)
		serverMonitor.serverMonitorRepository.putSession(s)
	}
}
func (serverMonitor *ServerMonitor) addInflight(connProxyID string) {
	serverMonitor.Lock()
	defer serverMonitor.Unlock()
	if s, ok := serverMonitor.serverMonitorRepository.getSession(connProxyID); ok {
		s.inflightLen++
		serverMonitor.serverMonitorRepository.putSession(s)
	}
}
func (serverMonitor *ServerMonitor) delInflight(connProxyID string) {
	serverMonitor.Lock()
	defer serverMonitor.Unlock()
	if s, ok := serverMonitor.serverMonitorRepository.getSession(connProxyID); ok {
		s.inflightLen--
		serverMonitor.serverMonitorRepository.putSession(s)
	}
}
func (serverMonitor *ServerMonitor) msgEnQueue(connProxyID string) {
	serverMonitor.Lock()
	defer serverMonitor.Unlock()
	if s, ok := serverMonitor.serverMonitorRepository.getSession(connProxyID); ok {
		s.msgQueueLen++
		serverMonitor.serverMonitorRepository.putSession(s)
	}
}
func (serverMonitor *ServerMonitor) msgDeQueue(connProxyID string) {
	serverMonitor.Lock()
	defer serverMonitor.Unlock()
	if s, ok := serverMonitor.serverMonitorRepository.getSession(connProxyID); ok {
		s.msgQueueLen--
		serverMonitor.serverMonitorRepository.putSession(s)
	}
}
func (serverMonitor *ServerMonitor) msgQueueDropped(connProxyID string) {
	serverMonitor.Lock()
	defer serverMonitor.Unlock()
	if s, ok := serverMonitor.serverMonitorRepository.getSession(connProxyID); ok {
		s.msgQueueDropped++
		serverMonitor.serverMonitorRepository.putSession(s)
	}
}

func (serverMonitor *ServerMonitor) connProxys() ConnProxyList {
	serverMonitor.Lock()
	defer serverMonitor.Unlock()
	return serverMonitor.serverMonitorRepository.getConnProxyInfos()
}

func (serverMonitor *ServerMonitor) GetconnProxy(connProxyID string) (ConnProxyInfo, bool) {
	serverMonitor.Lock()
	defer serverMonitor.Unlock()
	return serverMonitor.serverMonitorRepository.getconnProxyInfo(connProxyID)
}

//Sessions returns the session info for all  sessions
func (serverMonitor *ServerMonitor) Sessions() SessionList {
	serverMonitor.Lock()
	defer serverMonitor.Unlock()
	return serverMonitor.serverMonitorRepository.getSessions()
}

func (serverMonitor *ServerMonitor) GetSession(connProxyID string) (SessionInfo, bool) {
	serverMonitor.Lock()
	defer serverMonitor.Unlock()
	return serverMonitor.serverMonitorRepository.getSession(connProxyID)
}

func (serverMonitor *ServerMonitor) connProxySubscriptions(connProxyID string) SubscriptionList {
	serverMonitor.Lock()
	defer serverMonitor.Unlock()
	return serverMonitor.serverMonitorRepository.getConnProxySubscriptions(connProxyID)
}

func (serverMonitor *ServerMonitor) Subscriptions() SubscriptionList {
	serverMonitor.Lock()
	defer serverMonitor.Unlock()
	return serverMonitor.serverMonitorRepository.getSubscriptions()
}

type SubscriptionsInfo struct {
	connProxyID string    `json:"connProxy_id"`
	qos      uint8     `json:"qos"`
	name     string    `json:"name"`
	time       time.Time `json:"at"`
}

type SubscriptionList []SubscriptionsInfo

func (s SubscriptionList) Len() int           { return len(s) }
func (s SubscriptionList) Less(i, j int) bool { return s[i].time.UnixNano() <= s[j].time.UnixNano() }
func (s SubscriptionList) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

//客户端连接类型
type ConnProxyInfo struct {
	connProxyID  string    `json:"connProxy_id"`
	username     string    `json:"username"`
	remoteAddr   string    `json:"remote_addr"`
	cleanSession bool      `json:"clean_session"`
	keepAlive    uint16    `json:"keep_alive"`
	connectedTime  time.Time `json:"connected_at"`
}

type ConnProxyList []ConnProxyInfo

func (c ConnProxyList) Len() int { return len(c) }
func (c ConnProxyList) Less(i, j int) bool {
	return c[i].connectedTime.UnixNano() <= c[j].connectedTime.UnixNano()
}
func (c ConnProxyList) Swap(i, j int) { c[i], c[j] = c[j], c[i] }

// 客户端连接session定义
type SessionInfo struct {
	connProxyID        string    `json:"connProxy_id"`
	status          string    `json:"status"`
	remoteAddr      string    `json:"remote_addr"`
	cleanSession    bool      `json:"clean_session"`
	subscriptions   int       `json:"subscriptions"`
	maxInflight     int       `json:"max_inflight"`
	inflightLen     int       `json:"inflight_len"`
	maxMsgQueue     int       `json:"max_msg_queue"`
	msgQueueLen     int       `json:"msg_queue_len"`
	msgQueueDropped int       `json:"msg_queue_dropped"`
	connectedTime     time.Time `json:"connected_at"`
	offlineTime       time.Time `json:"offline_at,omitempty"`
}

type SessionList []SessionInfo

func (s SessionList) Len() int { return len(s) }
func (s SessionList) Less(i, j int) bool {
	return s[i].connectedTime.UnixNano() <= s[j].connectedTime.UnixNano()
}
func (s SessionList) Swap(i, j int) { s[i], s[j] = s[j], s[i] }

func (serverMonitorStore *ServerMonitorStore) PutconnProxy(info ConnProxyInfo) {
	serverMonitorStore.connProxyInfoMap[info.connProxyID] = info
}

func (serverMonitorStore *ServerMonitorStore) GetconnProxy(connProxyID string) (ConnProxyInfo, bool) {
	info, ok := serverMonitorStore.connProxyInfoMap[connProxyID]
	return info, ok
}

func (serverMonitorStore *ServerMonitorStore) connProxys() ConnProxyList {
	mlen := len(serverMonitorStore.connProxyInfoMap)
	if mlen == 0 {
		return nil
	}
	list := make(ConnProxyList, 0, mlen)
	for _, v := range serverMonitorStore.connProxyInfoMap {
		list = append(list, v)
	}
	sort.Sort(list)
	return list
}

func (serverMonitorStore *ServerMonitorStore) DelconnProxy(connProxyID string) {
	delete(serverMonitorStore.connProxyInfoMap, connProxyID)
}

func (serverMonitorStore *ServerMonitorStore) PutSession(info SessionInfo) {
	serverMonitorStore.sessions[info.connProxyID] = info
}

func (serverMonitorStore *ServerMonitorStore) GetSession(connProxyID string) (SessionInfo, bool) {
	s, ok := serverMonitorStore.sessions[connProxyID]
	return s, ok
}

func (serverMonitorStore *ServerMonitorStore) Sessions() SessionList {
	mlen := len(serverMonitorStore.sessions)
	if mlen == 0 {
		return nil
	}
	list := make(SessionList, 0, mlen)
	for _, v := range serverMonitorStore.sessions {
		list = append(list, v)
	}
	sort.Sort(list)
	return list
}

func (serverMonitorStore *ServerMonitorStore) DelSession(connProxyID string) {
	delete(serverMonitorStore.sessions, connProxyID)
}

func (serverMonitorStore *ServerMonitorStore) connProxySubscriptions(connProxyID string) SubscriptionList {
	mlen := len(serverMonitorStore.subscriptions[connProxyID])
	if mlen == 0 {
		return nil
	}
	list := make(SubscriptionList, 0, mlen)
	for _, v := range serverMonitorStore.subscriptions[connProxyID] {
		list = append(list, v)
	}
	sort.Sort(list)
	return list
}

func (serverMonitorStore *ServerMonitorStore) DelconnProxySubscriptions(connProxyID string) {
	delete(serverMonitorStore.subscriptions, connProxyID)
}

func (serverMonitorStore *ServerMonitorStore) PutSubscription(info SubscriptionsInfo) {
	if _, ok := serverMonitorStore.subscriptions[info.connProxyID]; !ok {
		serverMonitorStore.subscriptions[info.connProxyID] = make(map[string]SubscriptionsInfo)
	}
	serverMonitorStore.subscriptions[info.connProxyID][info.name] = info
}

func (serverMonitorStore *ServerMonitorStore) DelSubscription(connProxyID string, topicName string) {
	if _, ok := serverMonitorStore.subscriptions[connProxyID]; ok {
		delete(serverMonitorStore.subscriptions[connProxyID], topicName)
	}
}

func (serverMonitorStore *ServerMonitorStore) Subscriptions() SubscriptionList {
	mlen := len(serverMonitorStore.subscriptions)
	if mlen == 0 {
		return nil
	}
	list := make(SubscriptionList, 0, mlen)
	for k := range serverMonitorStore.subscriptions {
		for _, vv := range serverMonitorStore.subscriptions[k] {
			list = append(list, vv)
		}
	}
	sort.Sort(list)
	return list
}

func (serverMonitorStore *ServerMonitorStore) Open() error {
	return nil
}

func (serverMonitorStore *ServerMonitorStore) Close() error {
	return nil
}

