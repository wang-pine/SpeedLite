package udpnative

import (
	"bytes"
	"encoding/binary"
	"net"
	"testing"
	"time"
)

func TestMakePacketData(t *testing.T) {
	payload := []byte{1, 2, 3, 4}
	pkt := makePacket(42, 7, 123456789, 0, payload)
	if len(pkt) != hdrLen+len(payload) {
		t.Fatalf("len=%d, want %d", len(pkt), hdrLen+len(payload))
	}
	if !bytes.Equal(pkt[:4], magic) {
		t.Fatal("magic mismatch")
	}
	if binary.BigEndian.Uint32(pkt[4:8]) != 42 {
		t.Fatal("session mismatch")
	}
	if binary.BigEndian.Uint64(pkt[8:16]) != 7 {
		t.Fatal("seq mismatch")
	}
	if binary.BigEndian.Uint64(pkt[16:24]) != 123456789 {
		t.Fatal("ts mismatch")
	}
	if !bytes.Equal(pkt[hdrLen:], payload) {
		t.Fatal("payload mismatch")
	}
}

func TestMakePacketControl(t *testing.T) {
	pkt := makePacket(9, 0, 0, ctrlHello, nil)
	if len(pkt) != hdrLen+2 {
		t.Fatalf("ctrl pkt len=%d", len(pkt))
	}
	if pkt[hdrLen] != ctrlHello {
		t.Fatal("ctrl type mismatch")
	}
}

func TestSessionLossStats(t *testing.T) {
	// 用本地 socket 模拟一次真实收发：发送 100 个包，接收端统计丢包
	server, err := NewServer("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()

	raddr, err := net.ResolveUDPAddr("udp", server.Addr())
	if err != nil {
		t.Fatal(err)
	}
	conn, err := net.DialUDP("udp", nil, raddr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	const sessID = 1
	// HELLO (up direction)
	hello := makePacket(sessID, 0, 0, ctrlHello, []byte{dirUp})
	if _, err := conn.Write(hello); err != nil {
		t.Fatal(err)
	}
	// 等 ACK
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	ackBuf := make([]byte, 128)
	n, err := conn.Read(ackBuf)
	if err != nil {
		t.Fatalf("no ack: %v", err)
	}
	if n < hdrLen+2 || ackBuf[hdrLen] != ctrlAck {
		t.Fatalf("bad ack: n=%d type=%d", n, ackBuf[hdrLen])
	}

	// 发送 100 个数据包，seq 1..100
	payload := make([]byte, 1400-hdrLen)
	for i := uint64(1); i <= 100; i++ {
		pkt := makePacket(sessID, i, time.Now().UnixNano(), 0, payload)
		if _, err := conn.Write(pkt); err != nil {
			t.Fatal(err)
		}
	}
	// 模拟丢包：跳跃 seq（101, 103, 104...）—— 丢 102
	seq := uint64(101)
	for i := 0; i < 5; i++ {
		pkt := makePacket(sessID, seq, time.Now().UnixNano(), 0, payload)
		conn.Write(pkt)
		if i == 0 {
			seq += 2 // 跳 102
		} else {
			seq++
		}
	}
	time.Sleep(200 * time.Millisecond)

	// DONE -> 请求结果
	done := makePacket(sessID, 0, 0, ctrlDone, nil)
	conn.Write(done)
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	resBuf := make([]byte, 2048)
	n, err = conn.Read(resBuf)
	if err != nil {
		t.Fatalf("no result: %v", err)
	}
	if n < hdrLen+2 || resBuf[hdrLen] != ctrlResult {
		t.Fatalf("bad result frame: type=%d", resBuf[hdrLen])
	}
	// 105 packets sent (100 + 5), lost = 1 (seq 102)
	// 检查服务器状态（key 用客户端本地地址）
	clientAddr := conn.LocalAddr().(*net.UDPAddr)
	key := sessKey(sessID, clientAddr)
	server.mu.Lock()
	ss := server.sess[key]
	server.mu.Unlock()
	if ss == nil {
		t.Fatal("session not found")
	}
	ss.mu.Lock()
	if ss.got != 105 {
		t.Fatalf("got=%d want 105", ss.got)
	}
	if ss.lost != 1 {
		t.Fatalf("lost=%d want 1", ss.lost)
	}
	ss.mu.Unlock()
}
