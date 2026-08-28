// Command speedtest-server 是测速服务端：托管 Web 前端 + WebSocket(TCP) 测速 +
// 原生 TCP/UDP 测速端口（供 CLI 使用）。
//
// 端口约定（均可通过 flags 修改）：
//
//	-http  8080  Web 页面 + WebSocket 测速（+ WebRTC 信令）
//	-tcp   5001  原生 TCP 测速（speedctl 使用）
//	-udp   5201  原生 UDP 测速（speedctl 使用）
package main

import (
	"embed"
	"flag"
	"fmt"
	"io/fs"
	"log"
	"net/http"

	"speedTest/internal/rtcbridge"
	"speedTest/internal/tcpnative"
	"speedTest/internal/udpnative"
	"speedTest/internal/version"
	"speedTest/internal/wsstream"
)

//go:embed web
var webFS embed.FS

var (
	httpAddr = flag.String("http", ":8080", "Web/WebSocket 监听地址")
	tcpAddr  = flag.String("tcp", ":5001", "原生 TCP 测速监听地址")
	udpAddr  = flag.String("udp", ":5201", "原生 UDP 测速监听地址")
	showVer  = flag.Bool("version", false, "显示版本并退出")
)

func main() {
	flag.Parse()

	if *showVer {
		fmt.Printf("speedtest-server %s\n", version.Version)
		return
	}

	// 静态页面（嵌入二进制）
	sub, err := fs.Sub(webFS, "web")
	if err != nil {
		log.Fatalf("embed sub: %v", err)
	}
	fileServer := http.FileServer(http.FS(sub))
	http.Handle("/", fileServer)

	// WebSocket 测速
	http.HandleFunc("/ws/test", wsstream.HandleWS)

	// WebRTC 信令（UDP 测速）
	http.HandleFunc("/ws/signal", rtcbridge.HandleSignal)

	// 原生 TCP
	if _, err := tcpnative.Listen(*tcpAddr); err != nil {
		log.Fatalf("tcp-native listen %s: %v", *tcpAddr, err)
	}

	// 原生 UDP
	udpServer, err := udpnative.NewServer(*udpAddr)
	if err != nil {
		log.Fatalf("udp listen %s: %v", *udpAddr, err)
	}
	defer udpServer.Close()
	log.Printf("[udp-native] listening on %s", *udpAddr)

	log.Printf("speedtest-server v%s 启动", version.Version)
	log.Printf("  Web/WS:   http://localhost%s  (页面 + /ws/test)", *httpAddr)
	log.Printf("  原生TCP:  %s  (speedctl)", *tcpAddr)
	log.Printf("  原生UDP:  %s  (speedctl)", *udpAddr)
	if err := http.ListenAndServe(*httpAddr, nil); err != nil {
		log.Fatal(err)
	}
}
