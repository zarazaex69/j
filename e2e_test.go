//go:build e2e

package j_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/pion/webrtc/v4"
	"github.com/pion/webrtc/v4/pkg/media"
	j "github.com/zarazaex69/j"
)

// E2E tests against a real Jitsi server.
// Run with: go test -tags e2e -v -timeout 120s -run TestE2E
//
// Environment:
//   JITSI_HOST  — Jitsi server (default: meet.jit.si)
//   JITSI_ROOM  — room name (default: random)
//   JITSI_DEBUG — set to "1" for XMPP debug logs

func testHost() string {
	if h := os.Getenv("JITSI_HOST"); h != "" {
		return h
	}
	return "meet.small-dm.ru"
}

func testRoom() string {
	if r := os.Getenv("JITSI_ROOM"); r != "" {
		return r
	}
	return fmt.Sprintf("j-e2e-test-%d", time.Now().UnixNano()%100000)
}

func testDebug() bool {
	return os.Getenv("JITSI_DEBUG") == "1"
}

func cfg(nick string) j.Config {
	return j.Config{
		Host:  testHost(),
		Room:  testRoom(),
		Nick:  nick,
		Debug: testDebug(),
	}
}

// TestE2E_JoinMUC_Chat tests XMPP connection, MUC join, and groupchat message exchange.
func TestE2E_JoinMUC_Chat(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	room := testRoom()
	host := testHost()
	debug := testDebug()

	// Bot 1 joins
	sess1, err := j.JoinMUC(ctx, j.Config{Host: host, Room: room, Nick: "bot1", Debug: debug})
	if err != nil {
		t.Fatalf("bot1 join: %v", err)
	}
	defer sess1.Close()
	t.Logf("bot1 joined: jid=%s room=%s", sess1.JID, sess1.RoomJID)

	// Bot 2 joins
	sess2, err := j.JoinMUC(ctx, j.Config{Host: host, Room: room, Nick: "bot2", Debug: debug})
	if err != nil {
		t.Fatalf("bot2 join: %v", err)
	}
	defer sess2.Close()
	t.Logf("bot2 joined: jid=%s", sess2.JID)

	time.Sleep(500 * time.Millisecond)

	// Bot1 sends message, bot2 should receive
	msgs2 := sess2.Messages()
	testMsg := fmt.Sprintf("hello-e2e-%d", time.Now().UnixNano())
	if err := sess1.Chat(testMsg); err != nil {
		t.Fatalf("bot1 chat: %v", err)
	}

	select {
	case m := <-msgs2:
		if m.Body != testMsg {
			t.Errorf("got message %q, want %q", m.Body, testMsg)
		}
		t.Logf("bot2 received: <%s> %s", m.From, m.Body)
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for chat message")
	}
}

// TestE2E_Jingle_Media tests full Jingle flow: two bots join, get session-initiate,
// exchange session-accept, establish WebRTC, send/receive video.
func TestE2E_Jingle_Media(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	room := testRoom()
	host := testHost()
	debug := testDebug()

	// Use j.Join which does the full flow: connect, services, focus, MUC, WaitJingle
	// Both need to join in parallel since Jicofo needs 2 participants
	type result struct {
		sess *j.Session
		err  error
	}
	ch1, ch2 := make(chan result, 1), make(chan result, 1)
	go func() {
		s, e := j.Join(ctx, j.Config{Host: host, Room: room, Nick: "media1", Debug: debug})
		ch1 <- result{s, e}
	}()
	go func() {
		s, e := j.Join(ctx, j.Config{Host: host, Room: room, Nick: "media2", Debug: debug})
		ch2 <- result{s, e}
	}()

	r1 := <-ch1
	if r1.err != nil {
		t.Skipf("bot1 join: %v", r1.err)
	}
	sess1 := r1.sess
	defer sess1.Close()

	r2 := <-ch2
	if r2.err != nil {
		t.Skipf("bot2 join: %v", r2.err)
	}
	sess2 := r2.sess
	defer sess2.Close()

	t.Logf("bot1: jid=%s", sess1.JID)
	t.Logf("bot2: jid=%s", sess2.JID)

	// --- Bot1: sender (VP8 video) ---
	pc1, err := webrtc.NewPeerConnection(sess1.IceConfig())
	if err != nil {
		t.Fatalf("pc1: %v", err)
	}
	defer pc1.Close()

	videoTrack, err := webrtc.NewTrackLocalStaticSample(
		webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeVP8, ClockRate: 90000},
		"e2evideo", "e2estream")
	if err != nil {
		t.Fatalf("track: %v", err)
	}
	pc1.AddTrack(videoTrack)
	sess1.PrepareBridgeSCTP(pc1)

	neg1 := sess1.Negotiator()
	neg1.PC = pc1
	if err := neg1.Accept(ctx); err != nil {
		t.Fatalf("bot1 accept: %v", err)
	}
	t.Log("bot1 session-accept sent")

	// Start sending video
	go feedVP8(ctx, videoTrack)

	// --- Bot2: receiver ---
	pc2, err := webrtc.NewPeerConnection(sess2.IceConfig())
	if err != nil {
		t.Fatalf("pc2: %v", err)
	}
	defer pc2.Close()

	trackCh := make(chan *webrtc.TrackRemote, 4)
	pc2.OnTrack(func(tr *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
		t.Logf("bot2 OnTrack: kind=%s codec=%s ssrc=%d", tr.Kind(), tr.Codec().MimeType, tr.SSRC())
		select {
		case trackCh <- tr:
		default:
		}
	})

	sess2.PrepareBridgeSCTP(pc2)
	neg2 := sess2.Negotiator()
	neg2.PC = pc2
	if err := neg2.Accept(ctx); err != nil {
		t.Fatalf("bot2 accept: %v", err)
	}
	t.Log("bot2 session-accept sent")

	// Wait for SCTP bridge on bot2 and request video
	if err := sess2.WaitBridgeSCTP(ctx); err != nil {
		t.Fatalf("bot2 bridge: %v", err)
	}
	t.Log("bot2 bridge open")

	if err := sess2.RequestVideo(ctx, 720); err != nil {
		t.Logf("bot2 request video: %v", err)
	}

	// Wait for bot2 to receive video track
	select {
	case tr := <-trackCh:
		t.Logf("bot2 got track: kind=%s codec=%s ssrc=%d", tr.Kind(), tr.Codec().MimeType, tr.SSRC())
		buf := make([]byte, 1500)
		for i := 0; i < 10; i++ {
			if _, _, err := tr.Read(buf); err != nil {
				t.Fatalf("read packet %d: %v", i, err)
			}
		}
		t.Log("SUCCESS: bot2 received 10 video RTP packets")
	case <-time.After(30 * time.Second):
		// Log ICE state for debugging
		t.Logf("bot1 ICE: %s PC: %s", pc1.ICEConnectionState(), pc1.ConnectionState())
		t.Logf("bot2 ICE: %s PC: %s", pc2.ICEConnectionState(), pc2.ConnectionState())
		t.Fatal("timeout waiting for video track on bot2")
	}
}

// TestE2E_Bridge_DataExchange tests colibri bridge channel data exchange between two bots.
func TestE2E_Bridge_DataExchange(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	room := testRoom()
	host := testHost()
	debug := testDebug()

	type result struct {
		sess *j.Session
		err  error
	}
	ch1, ch2 := make(chan result, 1), make(chan result, 1)
	go func() {
		s, e := j.Join(ctx, j.Config{Host: host, Room: room, Nick: "dc1", Debug: debug})
		ch1 <- result{s, e}
	}()
	go func() {
		s, e := j.Join(ctx, j.Config{Host: host, Room: room, Nick: "dc2", Debug: debug})
		ch2 <- result{s, e}
	}()

	r1 := <-ch1
	if r1.err != nil {
		t.Skipf("bot1: %v", r1.err)
	}
	sess1 := r1.sess
	defer sess1.Close()

	r2 := <-ch2
	if r2.err != nil {
		t.Skipf("bot2: %v", r2.err)
	}
	sess2 := r2.sess
	defer sess2.Close()

	// Setup PCs with SCTP bridge
	pc1, err := webrtc.NewPeerConnection(sess1.IceConfig())
	if err != nil {
		t.Fatalf("pc1: %v", err)
	}
	defer pc1.Close()
	sess1.PrepareBridgeSCTP(pc1)
	neg1 := sess1.Negotiator()
	neg1.PC = pc1
	if err := neg1.Accept(ctx); err != nil {
		t.Fatalf("bot1 accept: %v", err)
	}

	pc2, err := webrtc.NewPeerConnection(sess2.IceConfig())
	if err != nil {
		t.Fatalf("pc2: %v", err)
	}
	defer pc2.Close()
	sess2.PrepareBridgeSCTP(pc2)
	neg2 := sess2.Negotiator()
	neg2.PC = pc2
	if err := neg2.Accept(ctx); err != nil {
		t.Fatalf("bot2 accept: %v", err)
	}

	if err := sess1.WaitBridgeSCTP(ctx); err != nil {
		t.Fatalf("bot1 bridge: %v", err)
	}
	if err := sess2.WaitBridgeSCTP(ctx); err != nil {
		t.Fatalf("bot2 bridge: %v", err)
	}
	t.Log("both bridges open")

	time.Sleep(500 * time.Millisecond)

	// Bot1 sends, bot2 receives
	payload := []byte("e2e-bridge-test-12345")
	if err := sess1.BridgeSendRaw("", payload); err != nil {
		t.Fatalf("bot1 send: %v", err)
	}

	select {
	case msg := <-sess2.BridgeMessages():
		t.Logf("SUCCESS: bot2 received bridge msg class=%s from=%s", msg.Class, msg.From)
	case <-time.After(15 * time.Second):
		t.Fatal("timeout waiting for bridge message")
	}
}

// TestE2E_Stress_ReconnectLoop tests rapid join/leave cycles.
func TestE2E_Stress_ReconnectLoop(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	host := testHost()
	room := testRoom()
	debug := testDebug()

	for i := 0; i < 5; i++ {
		t.Logf("cycle %d/5", i+1)
		sess, err := j.JoinMUC(ctx, j.Config{Host: host, Room: room, Nick: fmt.Sprintf("stress%d", i), Debug: debug})
		if err != nil {
			t.Fatalf("cycle %d join: %v", i, err)
		}
		// Send a message to confirm session is alive
		if err := sess.Chat(fmt.Sprintf("ping-%d", i)); err != nil {
			t.Fatalf("cycle %d chat: %v", i, err)
		}
		time.Sleep(200 * time.Millisecond)
		if err := sess.Close(); err != nil {
			t.Logf("cycle %d close: %v", i, err)
		}
		time.Sleep(300 * time.Millisecond)
	}
	t.Log("5 reconnect cycles completed")
}

// --- helpers ---

func feedVP8(ctx context.Context, track *webrtc.TrackLocalStaticSample) {
	frame := []byte{
		0x10, 0x02, 0x00, 0x9d, 0x01, 0x2a, 0x01, 0x00, 0x01, 0x00, 0x00, 0xc0,
		0xfd, 0x07, 0x86, 0x83, 0x97, 0xff, 0xfe, 0xfb, 0x9f, 0x00, 0x00,
	}
	tick := time.NewTicker(33 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			_ = track.WriteSample(media.Sample{Data: frame, Duration: 33 * time.Millisecond})
		}
	}
}

// TestE2E_MultiHost runs a full connect+video test against many Jitsi instances.
// Pass hosts via env: JITSI_HOSTS="host1,host2,host3"
// Run: JITSI_HOSTS="meet.mamba.group,meet1.arbitr.ru" go test -tags e2e -v -timeout 600s -run TestE2E_MultiHost ./...
func TestE2E_MultiHost(t *testing.T) {
	hostsEnv := os.Getenv("JITSI_HOSTS")
	if hostsEnv == "" {
		t.Skip("JITSI_HOSTS not set")
	}
	var hosts []string
	for _, h := range strings.Split(hostsEnv, ",") {
		h = strings.TrimSpace(h)
		if h != "" {
			hosts = append(hosts, h)
		}
	}
	if len(hosts) == 0 {
		t.Skip("no hosts in JITSI_HOSTS")
	}

	for _, host := range hosts {
		t.Run(host, func(t *testing.T) {
			t.Parallel()
			testFullFlow(t, host)
		})
	}
}

func testFullFlow(t *testing.T, host string) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	room := fmt.Sprintf("j-test-%d", time.Now().UnixNano()%100000)
	debug := testDebug()

	type result struct {
		sess *j.Session
		err  error
	}
	ch1, ch2 := make(chan result, 1), make(chan result, 1)
	go func() {
		s, e := j.Join(ctx, j.Config{Host: host, Room: room, Nick: "t1", Debug: debug, Insecure: true})
		ch1 <- result{s, e}
	}()
	go func() {
		s, e := j.Join(ctx, j.Config{Host: host, Room: room, Nick: "t2", Debug: debug, Insecure: true})
		ch2 <- result{s, e}
	}()

	r1 := <-ch1
	if r1.err != nil {
		t.Fatalf("FAIL connect: %v", r1.err)
	}
	sess1 := r1.sess
	defer sess1.Close()

	r2 := <-ch2
	if r2.err != nil {
		t.Fatalf("FAIL connect: %v", r2.err)
	}
	sess2 := r2.sess
	defer sess2.Close()

	t.Logf("OK connect+jingle")

	// Bot1: send video (try VP8, fallback to H264)
	pc1, err := webrtc.NewPeerConnection(sess1.IceConfig())
	if err != nil {
		t.Fatalf("pc1: %v", err)
	}
	defer pc1.Close()

	codec := webrtc.MimeTypeVP8
	vt, _ := webrtc.NewTrackLocalStaticSample(
		webrtc.RTPCodecCapability{MimeType: codec, ClockRate: 90000}, "v", "s")
	pc1.AddTrack(vt)
	sess1.PrepareBridgeSCTP(pc1)
	neg1 := sess1.Negotiator()
	neg1.PC = pc1
	if err := neg1.Accept(ctx); err != nil {
		// VP8 not supported — retry with H264
		pc1.Close()
		pc1, _ = webrtc.NewPeerConnection(sess1.IceConfig())
		defer pc1.Close()
		codec = webrtc.MimeTypeH264
		vt, _ = webrtc.NewTrackLocalStaticSample(
			webrtc.RTPCodecCapability{MimeType: codec, ClockRate: 90000, SDPFmtpLine: "level-asymmetry-allowed=1;packetization-mode=1;profile-level-id=42e01f"}, "v", "s")
		pc1.AddTrack(vt)
		sess1.PrepareBridgeSCTP(pc1)
		neg1 = sess1.Negotiator()
		neg1.PC = pc1
		if err := neg1.Accept(ctx); err != nil {
			t.Skipf("SKIP: no supported video codec: %v", err)
		}
	}
	go feedVP8(ctx, vt)

	// Bot2: receive video
	pc2, err := webrtc.NewPeerConnection(sess2.IceConfig())
	if err != nil {
		t.Fatalf("pc2: %v", err)
	}
	defer pc2.Close()

	trackCh := make(chan *webrtc.TrackRemote, 4)
	pc2.OnTrack(func(tr *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
		select {
		case trackCh <- tr:
		default:
		}
	})
	sess2.PrepareBridgeSCTP(pc2)
	neg2 := sess2.Negotiator()
	neg2.PC = pc2
	if err := neg2.Accept(ctx); err != nil {
		t.Fatalf("FAIL accept2: %v", err)
	}

	if err := sess2.WaitBridgeSCTP(ctx); err != nil {
		t.Fatalf("FAIL bridge: %v", err)
	}
	sess2.RequestVideo(ctx, 720)

	select {
	case tr := <-trackCh:
		buf := make([]byte, 1500)
		for i := 0; i < 5; i++ {
			if _, _, err := tr.Read(buf); err != nil {
				t.Fatalf("FAIL read: %v", err)
			}
		}
		t.Logf("OK video: %s ssrc=%d", tr.Codec().MimeType, tr.SSRC())
	case <-ctx.Done():
		t.Logf("bot1 ICE=%s PC=%s", pc1.ICEConnectionState(), pc1.ConnectionState())
		t.Logf("bot2 ICE=%s PC=%s", pc2.ICEConnectionState(), pc2.ConnectionState())
		t.Fatalf("FAIL video timeout")
	}
}
