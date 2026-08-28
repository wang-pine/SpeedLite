// Package udpnative 实现基于原生 UDP socket 的测速（iperf 风格）。
// 包协议（均为网络字节序大端）：
//
//	包首部 24B：magic[4]="SPDU" + sessionID[4] + seq[8] + tsNs[8]
//	HELLO 控制包：magic + session=0 + seq=0 + ts=0 + [1B type=1] + [1B direction 1=up 2=down] + [payload 校验数据]
//	ACK 控制包：  magic + session + seq=0 + ts=0 + [1B type=2]
//	DATA 数据包： magic + session + seq + ts + payload(全零)
//	DONE 控制包： magic + session + seq=0 + ts=0 + [1B type=3]
//	RESULT 控制包：magic + session + seq=0 + ts=0 + [1B type=4] + JSON结果
//
// 丢包统计：接收方按 seq 连续性判断丢失（seq 从 1 递增，每包 +1）。
// 抖动统计：接收方按相邻包 ts 差值的变化率计算（RFC 3550 Jitter 简化版）。
package udpnative

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"net"
	"sync"
	"time"

	"speedTest/internal/engine"
)

// magic 4 字节。
var magic = []byte{'S', 'P', 'D', 'U'}

const (
	hdrLen = 24
)

// 控制包类型。
const (
	ctrlHello  = 1
	ctrlAck    = 2
	ctrlDone   = 3
	ctrlResult = 4
)

// HELLO 包载荷布局（在 24B 包头之后）：
//
//	[0]   type(1B)=ctrlHello
//	[1]   direction(1B): 1=up 2=down
//	[2:10] duration(8B float64 秒)
//	[10:18] packetLen(8B uint64)
//	[18:]  保留
const helloPayloadLen = 26

// direction
const (
	dirUp   = 1 // 客户端 -> 服务器
	dirDown = 2 // 服务器 -> 客户端
)

// session 服务器侧状态。
type session struct {
	id         string
	dir        int
	addr       *net.UDPAddr
	dur        float64 // 秒
	plen       int     // payload 长度
	lastSeq    uint64
	got        uint64
	lost       uint64
	dupe       uint64
	outOfOrder uint64
	prevTs     int64
	prevDelta  int64
	jitter     float64 // ms
	mu         sync.Mutex
}

// Server 原生 UDP 测速服务器。
type Server struct {
	Conn    *net.UDPConn
	mu      sync.Mutex
	sess    map[string]*session
	udpData chan []byte // 内部复用 buffer 池
	bufPool sync.Pool
}

// NewServer 创建 UDP 服务器并开始监听。
func NewServer(addr string) (*Server, error) {
	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return nil, err
	}
	conn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		return nil, err
	}
	// 增大接收缓冲区：高速 UDP 打流时默认 SO_RCVBUF(约 200KB) 会被塞满导致丢包。
	// 尽量设到 16MB（内核上限通常为 rmem_max，此处尽力而为）。
	_ = conn.SetReadBuffer(16 * 1024 * 1024)
	_ = conn.SetWriteBuffer(16 * 1024 * 1024)
	s := &Server{
		Conn: conn,
		sess: make(map[string]*session),
		bufPool: sync.Pool{
			New: func() any { b := make([]byte, 65536); return &b },
		},
	}
	go s.loop()
	return s, nil
}

// Addr 返回监听地址。
func (s *Server) Addr() string { return s.Conn.LocalAddr().String() }

func (s *Server) loop() {
	buf := make([]byte, 65536)
	for {
		n, addr, err := s.Conn.ReadFromUDP(buf)
		if err != nil {
			return // 连接关闭
		}
		if n < hdrLen {
			continue
		}
		// 直接使用 buf（同步处理完才进入下一次读），避免每包拷贝。
		s.handle(buf[:n], addr)
	}
}

func (s *Server) handle(pkt []byte, addr *net.UDPAddr) {
	if !bytes.Equal(pkt[:4], magic) {
		return
	}
	sessID := binary.BigEndian.Uint32(pkt[4:8])
	seq := binary.BigEndian.Uint64(pkt[8:16])
	ts := int64(binary.BigEndian.Uint64(pkt[16:24]))

	// 控制包：seq==0 表示控制
	if seq == 0 && len(pkt) > hdrLen {
		ctype := pkt[24]
		switch ctype {
		case ctrlHello:
			direction := 0
			duration := 10.0
			packetLen := uint64(1400 - hdrLen)
			if len(pkt) >= hdrLen+2 {
				direction = int(pkt[25])
			}
			if len(pkt) >= hdrLen+2+8 {
				duration = math.Float64frombits(binary.BigEndian.Uint64(pkt[26:34]))
			}
			if len(pkt) >= hdrLen+2+8+8 {
				packetLen = binary.BigEndian.Uint64(pkt[34:42])
			}
			if duration <= 0 || duration > 600 {
				duration = 10
			}
			id := fmt.Sprintf("%d-%s", sessID, addr.String())
			key := sessKey(sessID, addr)
			s.mu.Lock()
			s.sess[key] = &session{
				id:    id,
				dir:   direction,
				addr:  addr,
				dur:   duration,
				plen:  int(packetLen),
			}
			s.mu.Unlock()
			// ACK
			ack := makePacket(sessID, 0, 0, ctrlAck, nil)
			_, _ = s.Conn.WriteToUDP(ack, addr)
			log.Printf("[udp] session %s registered dir=%d dur=%.1fs plen=%d from %s", id, direction, duration, packetLen, addr)
			// 下行：服务器主动发送
			if direction == dirDown {
				go s.sendDown(sessID, addr)
			}
			return
		case ctrlDone:
			key := sessKey(sessID, addr)
			s.mu.Lock()
			ss := s.sess[key]
			s.mu.Unlock()
			if ss != nil {
				s.sendResult(sessID, ss, addr)
			}
			return
		}
		return
	}

	// 数据包
	key := sessKey(sessID, addr)
	s.mu.Lock()
	ss := s.sess[key]
	s.mu.Unlock()
	if ss == nil {
		return
	}
	ss.mu.Lock()
	ss.got++
	if seq <= ss.lastSeq {
		ss.dupe++
		if seq < ss.lastSeq {
			ss.outOfOrder++
		}
	} else if ss.lastSeq > 0 && seq != ss.lastSeq+1 {
		ss.lost += seq - ss.lastSeq - 1
	}
	ss.lastSeq = seq
	if ss.prevTs != 0 {
		// jitter: |(ts_n - ts_{n-1}) - (prevDelta)|
		delta := ts - ss.prevTs
		if ss.prevDelta == 0 {
			ss.prevDelta = delta
		} else {
			d := math.Abs(float64(delta - ss.prevDelta))
			ss.jitter = (ss.jitter*15 + d/1e6) / 16 // ms, 指数平滑
			ss.prevDelta = delta
		}
	} else {
		ss.prevDelta = 0
	}
	ss.prevTs = ts
	ss.mu.Unlock()
}

func (s *Server) sendDown(sessID uint32, addr *net.UDPAddr) {
	s.mu.Lock()
	ss := s.sess[sessKey(sessID, addr)]
	s.mu.Unlock()
	if ss == nil {
		return
	}
	dur := time.Duration(ss.dur * float64(time.Second))
	plen := ss.plen
	if plen <= 0 || plen > 65507-hdrLen {
		plen = 1400 - hdrLen
	}
	end := time.Now().Add(dur)

	// 复用发送缓冲：只更新 seq/ts 字段
	pkt := make([]byte, hdrLen+plen)
	copy(pkt, magic)
	seq := uint64(1)
	for time.Now().Before(end) {
		binary.BigEndian.PutUint32(pkt[4:8], sessID)
		binary.BigEndian.PutUint64(pkt[8:16], seq)
		binary.BigEndian.PutUint64(pkt[16:24], uint64(time.Now().UnixNano()))
		_, err := s.Conn.WriteToUDP(pkt, addr)
		if err != nil {
			return
		}
		seq++
	}
	// 通知客户端下行结束
	done := makePacket(sessID, 0, 0, ctrlDone, nil)
	_, _ = s.Conn.WriteToUDP(done, addr)
}

func (s *Server) sendResult(sessID uint32, ss *session, addr *net.UDPAddr) {
	ss.mu.Lock()
	st := engine.UDPStats{
		Packets: ss.got,
		Lost:    ss.lost,
		Jitter:  ss.jitter,
	}
	ss.mu.Unlock()
	res := engine.Stats{
		Packets: st.Packets,
		Lost:    st.Lost,
		LostPct: st.LostPct(),
		Jitter:  st.Jitter,
	}
	b, _ := json.Marshal(res)
	pkt := makePacket(sessID, 0, 0, ctrlResult, b)
	_, _ = s.Conn.WriteToUDP(pkt, addr)
}

// makePacket 构造 UDP 包。ctrlType=0 表示数据包。
func makePacket(sessID uint32, seq uint64, tsNs int64, ctrlType byte, payload []byte) []byte {
	total := hdrLen
	if ctrlType != 0 {
		total += 2 // type + direction(或保留)
	}
	total += len(payload)
	buf := make([]byte, total)
	copy(buf, magic)
	binary.BigEndian.PutUint32(buf[4:8], sessID)
	binary.BigEndian.PutUint64(buf[8:16], seq)
	binary.BigEndian.PutUint64(buf[16:24], uint64(tsNs))
	off := hdrLen
	if ctrlType != 0 {
		buf[off] = ctrlType
		off++
		buf[off] = 0
		off++
	}
	copy(buf[off:], payload)
	return buf
}

func sessKey(sessID uint32, addr *net.UDPAddr) string {
	return fmt.Sprintf("%d|%s", sessID, addr.String())
}

// Close 关闭服务器。
func (s *Server) Close() {
	_ = s.Conn.Close()
}
