package main

import (
	"bufio"
	"context"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/pion/webrtc/v4"
	"github.com/pion/webrtc/v4/pkg/media"
	j "github.com/zarazaex69/j"
	"github.com/zarazaex69/j/internal/colibri"
	"github.com/zarazaex69/j/internal/peer"
)

func main() {
	host := flag.String("host", "", "Jitsi Meet server host")
	room := flag.String("room", "", "Room name")
	nick := flag.String("nick", "thejproject", "Display name")
	debug := flag.Bool("debug", false, "Verbose XMPP logging")
	chat := flag.Bool("chat", false, "Chat mode: join room and read stdin for messages")
	dc := flag.Bool("dc", false, "Bridge channel mode: stdin → broadcast EndpointMessage as text")
	dcRaw := flag.Bool("dc-raw", false, "Bridge channel raw mode: pipe stdin → bridge → stdout (binary, base64-framed)")
	media := flag.Bool("media", false, "Media mode: setup pion PeerConnection, send session-accept, print track events")
	sendVideo := flag.Bool("send-video", false, "(media mode) attach a sendonly VP8 track and announce it to Jicofo")
	bench := flag.Bool("bench", false, "Throughput benchmark: open colibri-ws, broadcast EndpointMessage at max rate, print Mbps")
	benchXMPP := flag.Bool("bench-xmpp", false, "Throughput benchmark via XMPP groupchat (path goes through Prosody, NOT through JVB)")
	benchSize := flag.Int("bench-size", 8192, "(bench) payload size per message in bytes")
	benchSecs := flag.Int("bench-secs", 30, "(bench) duration of the benchmark in seconds")
	timeout := flag.Duration("timeout", 5*time.Minute, "Timeout waiting for Jingle session")
	insecure := flag.Bool("insecure", false, "Skip TLS certificate verification")
	flag.Parse()

	if *host == "" || *room == "" {
		fmt.Fprintln(os.Stderr, "usage: cli -host meet.example.com -room myroom [-nick name] [-chat | -dc | -dc-raw]")
		os.Exit(1)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	fmt.Fprintf(os.Stderr, "joining %s/%s as %s...\n", *host, *room, *nick)

	switch {
	case *benchXMPP:
		runBenchXMPP(ctx, *host, *room, *nick, *debug, *insecure, *benchSize, *benchSecs)
	case *bench:
		runBench(ctx, *host, *room, *nick, *debug, *insecure, *timeout, *benchSize, *benchSecs)
	case *media:
		runMedia(ctx, *host, *room, *nick, *debug, *insecure, *timeout, *sendVideo)
	case *dcRaw:
		runDCRaw(ctx, *host, *room, *nick, *debug, *insecure, *timeout)
	case *dc:
		runDC(ctx, *host, *room, *nick, *debug, *insecure, *timeout)
	case *chat:
		runChat(ctx, *host, *room, *nick, *debug, *insecure, *timeout)
	default:
		runJingle(ctx, *host, *room, *nick, *debug, *insecure, *timeout)
	}
}

func runChat(ctx context.Context, host, room, nick string, debug, insecure bool, timeout time.Duration) {
	jctx, jcancel := context.WithTimeout(ctx, timeout)
	defer jcancel()

	sess, err := j.JoinMUC(jctx, j.Config{Host: host, Room: room, Nick: nick, Debug: debug, Insecure: insecure})
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	defer func() { _ = sess.Close() }()
	printServerAuth(sess)

	fmt.Fprintf(os.Stderr, "joined! type messages (/raise, /lower, /quit):\n")

	lines := readLines(ctx)
	incoming := sess.Messages()

	for {
		select {
		case <-ctx.Done():
			fmt.Fprintln(os.Stderr, "\nbye")
			return
		case m, ok := <-incoming:
			if !ok {
				return
			}
			fmt.Printf("<%s> %s\n", m.From, m.Body)
		case line, ok := <-lines:
			if !ok {
				return
			}
			if line == "" {
				continue
			}
			switch line {
			case "/quit", "/exit", "/leave":
				return
			case "/raise":
				if err := sess.RaiseHand(); err != nil {
					fmt.Fprintf(os.Stderr, "error: %v\n", err)
				}
			case "/lower":
				if err := sess.LowerHand(); err != nil {
					fmt.Fprintf(os.Stderr, "error: %v\n", err)
				}
			default:
				if err := sess.Chat(line); err != nil {
					fmt.Fprintf(os.Stderr, "send error: %v\n", err)
					return
				}
			}
		}
	}
}

func runJingle(ctx context.Context, host, room, nick string, debug, insecure bool, timeout time.Duration) {
	ctx, tcancel := context.WithTimeout(ctx, timeout)
	defer tcancel()

	sess, err := j.Join(ctx, j.Config{Host: host, Room: room, Nick: nick, Debug: debug, Insecure: insecure})
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	defer func() { _ = sess.Close() }()

	out := map[string]any{
		"jid":          sess.JID,
		"room_jid":     sess.RoomJID,
		"colibri_ws":   sess.ColibriWS,
		"sdp":          sess.SDP,
		"ice_servers":  sess.ICEServers,
		"candidates":   sess.Candidates,
		"data_channel": sess.DataChannel,
		"audio_ssrc":   sess.AudioSSRC,
		"video_ssrc":   sess.VideoSSRC,
		"server_auth":  sess.ServerAuth,
	}

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	_ = enc.Encode(out)
}

func printServerAuth(sess *j.Session) {
	auth := sess.ServerAuth
	authText := "off"
	if auth.AuthenticationRequired {
		authText = "on"
	}
	guestText := "no"
	if auth.GuestAccess {
		guestText = "yes"
	}
	externalText := "no"
	if auth.ExternalAuth {
		externalText = "yes"
	}
	fmt.Fprintf(os.Stderr, "server auth: %s, guest access: %s, external auth: %s\n", authText, guestText, externalText)
}

// runDC: text broadcast over bridge channel. Each line of stdin → EndpointMessage{text:line}.
func runDC(ctx context.Context, host, room, nick string, debug, insecure bool, timeout time.Duration) {
	jctx, jcancel := context.WithTimeout(ctx, timeout)
	defer jcancel()

	fmt.Fprintln(os.Stderr, "waiting for jingle session-initiate (needs 2nd participant in room)...")
	sess, err := j.Join(jctx, j.Config{Host: host, Room: room, Nick: nick, Debug: debug, Insecure: insecure})
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	defer func() { _ = sess.Close() }()
	printServerAuth(sess)

	if err := openBridgeAuto(ctx, sess); err != nil {
		fmt.Fprintf(os.Stderr, "open bridge: %v\n", err)
		os.Exit(1)
	}
	fmt.Fprintln(os.Stderr, "type messages to broadcast, /quit to exit:")

	go func() {
		for m := range sess.BridgeMessages() {
			fmt.Printf("[%s/%s] %s\n", m.Class, m.From, string(m.RawJSON))
		}
	}()

	lines := readLines(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case line, ok := <-lines:
			if !ok {
				return
			}
			if line == "/quit" || line == "/exit" {
				return
			}
			if err := sess.BridgeSendMessage("", map[string]any{"text": line}); err != nil {
				fmt.Fprintf(os.Stderr, "send: %v\n", err)
				return
			}
		}
	}
}

// runDCRaw: pipe arbitrary binary through the bridge.
//
//	stdin (binary) → SendRaw broadcast → other endpoint receives → DecodeRaw → stdout
func runDCRaw(ctx context.Context, host, room, nick string, debug, insecure bool, timeout time.Duration) {
	jctx, jcancel := context.WithTimeout(ctx, timeout)
	defer jcancel()

	fmt.Fprintln(os.Stderr, "waiting for jingle session-initiate (needs 2nd participant in room)...")
	sess, err := j.Join(jctx, j.Config{Host: host, Room: room, Nick: nick, Debug: debug, Insecure: insecure})
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	defer func() { _ = sess.Close() }()
	printServerAuth(sess)

	if err := openBridgeAuto(ctx, sess); err != nil {
		fmt.Fprintf(os.Stderr, "open bridge: %v\n", err)
		os.Exit(1)
	}

	// receive: decode raw frames to stdout, log other classes to stderr
	go func() {
		for m := range sess.BridgeMessages() {
			if raw := colibri.DecodeRaw(m); raw != nil {
				_, _ = os.Stdout.Write(raw)
				continue
			}
			if m.Class != "EndpointMessage" {
				fmt.Fprintf(os.Stderr, "[%s] %s\n", m.Class, string(m.RawJSON))
			}
		}
	}()

	// send: chunked stdin
	buf := make([]byte, 8192)
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		n, err := os.Stdin.Read(buf)
		if n > 0 {
			if serr := sess.BridgeSendRaw("", buf[:n]); serr != nil {
				fmt.Fprintf(os.Stderr, "send: %v\n", serr)
				return
			}
		}
		if err != nil {
			return
		}
	}
}

func runMedia(ctx context.Context, host, room, nick string, debug, insecure bool, timeout time.Duration, sendVideo bool) {
	jctx, jcancel := context.WithTimeout(ctx, timeout)
	defer jcancel()

	fmt.Fprintln(os.Stderr, "joining and waiting for jingle session-initiate...")
	sess, err := j.Join(jctx, j.Config{Host: host, Room: room, Nick: nick, Debug: debug, Insecure: insecure})
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	defer func() { _ = sess.Close() }()
	printServerAuth(sess)

	for round := 1; ; round++ {
		fmt.Fprintf(os.Stderr, "=== media round %d ===\n", round)
		if err := acceptOnce(ctx, sess, sendVideo); err != nil {
			fmt.Fprintf(os.Stderr, "round %d: %v\n", round, err)
		}

		// wait for next session-initiate (Jicofo "moving" or general-error reset)
		fmt.Fprintln(os.Stderr, "waiting for next session-initiate (reconnect)…")
		rictx, ricancel := context.WithTimeout(ctx, 30*time.Second)
		_, werr := sess.WaitJingleReinitiate(rictx)
		ricancel()
		if werr != nil {
			fmt.Fprintf(os.Stderr, "no reinitiate within timeout, exiting: %v\n", werr)
			return
		}
		fmt.Fprintln(os.Stderr, "got new session-initiate, re-accepting")
	}
}

// acceptOnce performs one full setup cycle: build pc, add tracks, Accept(),
// drain media, and wait until pc is closed/failed or ctx done.
func acceptOnce(ctx context.Context, sess *j.Session, sendVideo bool) error {
	pc, err := webrtc.NewPeerConnection(sess.IceConfig())
	if err != nil {
		return fmt.Errorf("new pc: %w", err)
	}
	defer func() { _ = pc.Close() }()

	if _, err := pc.AddTransceiverFromKind(webrtc.RTPCodecTypeAudio, webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionRecvonly}); err != nil {
		fmt.Fprintf(os.Stderr, "add audio recvonly: %v\n", err)
	}

	var localVideo *webrtc.TrackLocalStaticSample
	if sendVideo {
		localVideo, err = webrtc.NewTrackLocalStaticSample(
			webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeVP8, ClockRate: 90000},
			"jvideo", "jstream")
		if err != nil {
			return fmt.Errorf("new track: %w", err)
		}
		if _, err := pc.AddTrack(localVideo); err != nil {
			return fmt.Errorf("add track: %w", err)
		}
	} else {
		if _, err := pc.AddTransceiverFromKind(webrtc.RTPCodecTypeVideo, webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionRecvonly}); err != nil {
			fmt.Fprintf(os.Stderr, "add video recvonly: %v\n", err)
		}
	}

	pc.OnTrack(func(track *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
		fmt.Fprintf(os.Stderr, "[track] kind=%s id=%s codec=%s ssrc=%d\n",
			track.Kind(), track.ID(), track.Codec().MimeType, track.SSRC())
		buf := make([]byte, 1500)
		var pkts uint64
		for {
			_, _, err := track.Read(buf)
			if err != nil {
				fmt.Fprintf(os.Stderr, "[track ssrc=%d] closed: %v (rx=%d packets)\n", track.SSRC(), err, pkts)
				return
			}
			pkts++
		}
	})

	done := make(chan struct{}, 1)
	pc.OnICEConnectionStateChange(func(s webrtc.ICEConnectionState) {
		fmt.Fprintf(os.Stderr, "ICE: %s\n", s)
	})
	pc.OnConnectionStateChange(func(s webrtc.PeerConnectionState) {
		fmt.Fprintf(os.Stderr, "PC: %s\n", s)
		switch s {
		case webrtc.PeerConnectionStateFailed,
			webrtc.PeerConnectionStateDisconnected,
			webrtc.PeerConnectionStateClosed:
			select {
			case done <- struct{}{}:
			default:
			}
		}
	})

	neg := sess.Negotiator()
	neg.PC = pc
	if err := neg.Accept(ctx); err != nil {
		if peer.IsPlanBError(err) {
			fmt.Fprintln(os.Stderr, "detected Plan B offer, recreating PC with PlanB semantics...")
			_ = pc.Close()
			cfg := sess.IceConfig()
			cfg.SDPSemantics = webrtc.SDPSemanticsPlanB
			pc, err = webrtc.NewPeerConnection(cfg)
			if err != nil {
				return fmt.Errorf("new pc (planb): %w", err)
			}
			defer func() { _ = pc.Close() }()
			if _, err := pc.AddTransceiverFromKind(webrtc.RTPCodecTypeAudio, webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionRecvonly}); err != nil {
				fmt.Fprintf(os.Stderr, "add audio recvonly: %v\n", err)
			}
			if sendVideo {
				localVideo, _ = webrtc.NewTrackLocalStaticSample(
					webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeVP8, ClockRate: 90000},
					"jvideo", "jstream")
				pc.AddTrack(localVideo) //nolint:errcheck
			} else {
				pc.AddTransceiverFromKind(webrtc.RTPCodecTypeVideo, webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionRecvonly}) //nolint:errcheck
			}
			pc.OnTrack(func(track *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
				fmt.Fprintf(os.Stderr, "[track] kind=%s id=%s codec=%s ssrc=%d\n",
					track.Kind(), track.ID(), track.Codec().MimeType, track.SSRC())
				buf := make([]byte, 1500)
				var pkts uint64
				for {
					_, _, err := track.Read(buf)
					if err != nil {
						fmt.Fprintf(os.Stderr, "[track ssrc=%d] closed: %v (rx=%d packets)\n", track.SSRC(), err, pkts)
						return
					}
					pkts++
				}
			})
			pc.OnConnectionStateChange(func(s webrtc.PeerConnectionState) {
				fmt.Fprintf(os.Stderr, "PC: %s\n", s)
				switch s {
				case webrtc.PeerConnectionStateFailed,
					webrtc.PeerConnectionStateDisconnected,
					webrtc.PeerConnectionStateClosed:
					select {
					case done <- struct{}{}:
					default:
					}
				}
			})
			neg = sess.Negotiator()
			neg.PC = pc
			if err := neg.Accept(ctx); err != nil {
				return fmt.Errorf("accept (planb): %w", err)
			}
		} else {
			return fmt.Errorf("accept: %w", err)
		}
	}

	if sendVideo && localVideo != nil {
		fmt.Fprintln(os.Stderr, "video track included in session-accept SDP")
		go feedDummyVP8(ctx, localVideo)
	}

	// Request video from bridge — without this JVB won't forward any video
	if err := sess.RequestVideo(ctx, 720); err != nil {
		fmt.Fprintf(os.Stderr, "request video: %v\n", err)
	}

	fmt.Fprintln(os.Stderr, "session-accept sent")
	select {
	case <-done:
	case <-ctx.Done():
	}
	// Close PC immediately to stop pion's internal RTCP sender goroutine
	// before it writes to the already-closed DTLS pipe.
	_ = pc.Close()
	_ = neg.Terminate("success")
	return nil
}

// feedDummyVP8 writes a tiny black VP8 keyframe at 30fps. Used purely as a
// "we're alive" signal so the bridge can route our SSRC. Real apps would
// pipe ffmpeg / encoded frames here.
func feedDummyVP8(ctx context.Context, t *webrtc.TrackLocalStaticSample) {
	// 64x64 black VP8 keyframe (precomputed)
	frame := dummyVP8Keyframe()
	tick := time.NewTicker(33 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			_ = t.WriteSample(media.Sample{Data: frame, Duration: 33 * time.Millisecond})
		}
	}
}

// dummyVP8Keyframe returns a 1x1 black VP8 keyframe (well-formed, ~30 bytes).
// Generated once via libvpx; reused as filler data.
func dummyVP8Keyframe() []byte {
	return []byte{
		0x10, 0x02, 0x00, 0x9d, 0x01, 0x2a, 0x01, 0x00, 0x01, 0x00, 0x00, 0xc0,
		0xfd, 0x07, 0x86, 0x83, 0x97, 0xff, 0xfe, 0xfb, 0x9f, 0x00, 0x00,
	}
}

// runBench measures colibri-ws throughput. Bot 1 (this) sends, bot 2 (also -bench)
// receives. Stats printed every 2s plus a final summary.
func runBench(ctx context.Context, host, room, nick string, debug, insecure bool, timeout time.Duration, payloadSize, secs int) {
	jctx, jcancel := context.WithTimeout(ctx, timeout)
	defer jcancel()

	fmt.Fprintln(os.Stderr, "joining and waiting for jingle session-initiate...")
	sess, err := j.Join(jctx, j.Config{Host: host, Room: room, Nick: nick, Debug: debug, Insecure: insecure})
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	defer func() { _ = sess.Close() }()
	printServerAuth(sess)
	if err := openBridgeAuto(ctx, sess); err != nil {
		fmt.Fprintf(os.Stderr, "open bridge: %v\n", err)
		os.Exit(1)
	}

	fmt.Fprintf(os.Stderr, "bench: payload=%dB duration=%ds\n", payloadSize, secs)

	// payload (random-ish; doesn't really matter, JVB doesn't inspect)
	payload := make([]byte, payloadSize)
	for i := range payload {
		payload[i] = byte(i & 0xFF)
	}

	// fast-path: read raw WS frames, no JSON parse / base64 decode
	sess.Bridge().EnableRawMode()

	var rxBytes uint64
	var rxMsgs uint64
	go func() {
		for frame := range sess.Bridge().RawFrames() {
			rxBytes += uint64(len(frame))
			rxMsgs++
		}
	}()

	// stats ticker
	tickStop := make(chan struct{})
	go func() {
		var lastBytes uint64
		var lastMsgs uint64
		t := time.NewTicker(2 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-tickStop:
				return
			case <-t.C:
				bps := float64(rxBytes-lastBytes) * 8 / 2.0
				mps := rxMsgs - lastMsgs
				lastBytes = rxBytes
				lastMsgs = rxMsgs
				fmt.Fprintf(os.Stderr, "[bench] rx %.2f Mbit/s, %d msg/s (total: %d msgs, %.2f MB)\n",
					bps/1e6, mps/2, rxMsgs, float64(rxBytes)/1e6)
			}
		}
	}()

	// sender
	deadline := time.Now().Add(time.Duration(secs) * time.Second)
	var txBytes uint64
	var txMsgs uint64
	t0 := time.Now()
	for time.Now().Before(deadline) {
		if err := sess.BridgeSendRaw("", payload); err != nil {
			fmt.Fprintf(os.Stderr, "send err: %v\n", err)
			break
		}
		txBytes += uint64(payloadSize)
		txMsgs++
	}
	close(tickStop)
	dt := time.Since(t0).Seconds()

	fmt.Fprintf(os.Stderr, "\n=== bench results ===\n")
	fmt.Fprintf(os.Stderr, "duration:    %.2fs\n", dt)
	fmt.Fprintf(os.Stderr, "tx:          %d msgs, %.2f MB, %.2f Mbit/s, %.0f msg/s\n",
		txMsgs, float64(txBytes)/1e6, float64(txBytes)*8/dt/1e6, float64(txMsgs)/dt)
	fmt.Fprintf(os.Stderr, "rx (echoed): %d msgs, %.2f MB\n", rxMsgs, float64(rxBytes)/1e6)
}

// runBenchXMPP measures throughput of XMPP groupchat through Prosody (NOT through JVB).
// Path: client → wss://host/xmpp-websocket → prosody → other clients in MUC.
// Each message is a <message type="groupchat"><body>BASE64</body></message> stanza.
// Stanza size limit on cryptopro Prosody = 256 KB.
func runBenchXMPP(ctx context.Context, host, room, nick string, debug, insecure bool, payloadSize, secs int) {
	fmt.Fprintln(os.Stderr, "joining MUC (XMPP-only, no Jingle)...")
	sess, err := j.JoinMUC(ctx, j.Config{Host: host, Room: room, Nick: nick, Debug: debug, Insecure: insecure})
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	defer func() { _ = sess.Close() }()
	printServerAuth(sess)

	fmt.Fprintf(os.Stderr, "bench-xmpp: payload=%dB duration=%ds\n", payloadSize, secs)

	payload := make([]byte, payloadSize)
	for i := range payload {
		payload[i] = byte(i & 0xFF)
	}
	// pre-encode payload to base64 (we'll fake-vary the body)
	body := base64.StdEncoding.EncodeToString(payload)
	roomJID := sess.RoomJID
	// direct-chat target: MUC occupant JID. We pick the first other endpoint.
	// For groupchat use roomJID directly with type="groupchat".
	var target string
	for _, ep := range sess.Endpoints() {
		target = roomJID + "/" + ep
		break
	}
	if target == "" {
		fmt.Fprintln(os.Stderr, "no other endpoint in room — falling back to groupchat broadcast")
		target = roomJID
	}

	// rx counter via Messages channel — we get all groupchat messages,
	// payload size approximated by body length / b64 ratio
	var rxBytes uint64
	var rxMsgs uint64
	go func() {
		for m := range sess.Messages() {
			rxMsgs++
			// Body is base64; original size ≈ len(body)*3/4
			rxBytes += uint64(len(m.Body) * 3 / 4)
		}
	}()

	// stats ticker
	tickStop := make(chan struct{})
	go func() {
		var lastBytes, lastMsgs uint64
		t := time.NewTicker(2 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-tickStop:
				return
			case <-t.C:
				bps := float64(rxBytes-lastBytes) * 8 / 2.0
				mps := rxMsgs - lastMsgs
				lastBytes = rxBytes
				lastMsgs = rxMsgs
				fmt.Fprintf(os.Stderr, "[xmpp] rx %.2f Mbit/s, %d msg/s (total: %d msgs, %.2f MB)\n",
					bps/1e6, mps/2, rxMsgs, float64(rxBytes)/1e6)
			}
		}
	}()

	// sender — raw stanza via Conn.Send to skip xmlEscape on hot path
	xc := sess.LowLevel()
	deadline := time.Now().Add(time.Duration(secs) * time.Second)
	var txBytes uint64
	var txMsgs uint64
	t0 := time.Now()
	if payloadSize > 0 {
		msgType := "chat"
		if target == roomJID {
			msgType = "groupchat"
		}
		for time.Now().Before(deadline) {
			stanza := `<message to="` + target + `" type="` + msgType + `" xmlns="jabber:client"><body>` + body + `</body></message>`
			if err := xc.Send(stanza); err != nil {
				fmt.Fprintf(os.Stderr, "send err: %v\n", err)
				break
			}
			txBytes += uint64(payloadSize)
			txMsgs++
		}
	} else {
		// recv-only: just sleep until deadline
		select {
		case <-time.After(time.Until(deadline)):
		case <-ctx.Done():
		}
	}
	close(tickStop)
	dt := time.Since(t0).Seconds()

	fmt.Fprintf(os.Stderr, "\n=== bench-xmpp results ===\n")
	fmt.Fprintf(os.Stderr, "duration:    %.2fs\n", dt)
	fmt.Fprintf(os.Stderr, "tx:          %d msgs, %.2f MB, %.2f Mbit/s, %.0f msg/s\n",
		txMsgs, float64(txBytes)/1e6, float64(txBytes)*8/dt/1e6, float64(txMsgs)/dt)
	fmt.Fprintf(os.Stderr, "rx (echoed): %d msgs, %.2f MB\n", rxMsgs, float64(rxBytes)/1e6)
}

func readLines(ctx context.Context) <-chan string {
	out := make(chan string)
	go func() {
		defer close(out)
		scanner := bufio.NewScanner(os.Stdin)
		for scanner.Scan() {
			select {
			case out <- scanner.Text():
			case <-ctx.Done():
				return
			}
		}
	}()
	return out
}

// openBridgeAuto opens the bridge channel using colibri-ws if available,
// otherwise falls back to SCTP datachannel via a new PeerConnection.
func openBridgeAuto(ctx context.Context, sess *j.Session) error {
	if sess.ColibriWS != "" {
		if err := sess.OpenBridge(ctx); err == nil {
			fmt.Fprintln(os.Stderr, "bridge connected (colibri-ws)")
			return nil
		} else {
			fmt.Fprintf(os.Stderr, "colibri-ws failed (%v), falling back to SCTP...\n", err)
		}
	} else {
		fmt.Fprintln(os.Stderr, "no colibri-ws, using SCTP datachannel...")
	}
	return openBridgeSCTP(ctx, sess)
}

func openBridgeSCTP(ctx context.Context, sess *j.Session) error {
	cfg := sess.IceConfig()
	pc, err := webrtc.NewPeerConnection(cfg)
	if err != nil {
		return fmt.Errorf("new pc: %w", err)
	}
	if err := sess.PrepareBridgeSCTP(pc); err != nil {
		_ = pc.Close()
		return fmt.Errorf("prepare sctp: %w", err)
	}
	neg := sess.Negotiator()
	neg.PC = pc
	if err := neg.Accept(ctx); err != nil {
		_ = pc.Close()
		if peer.IsPlanBError(err) {
			fmt.Fprintln(os.Stderr, "detected Plan B offer, recreating PC with PlanB semantics...")
			cfg.SDPSemantics = webrtc.SDPSemanticsPlanB
			pc, err = webrtc.NewPeerConnection(cfg)
			if err != nil {
				return fmt.Errorf("new pc (planb): %w", err)
			}
			if err := sess.PrepareBridgeSCTP(pc); err != nil {
				_ = pc.Close()
				return fmt.Errorf("prepare sctp (planb): %w", err)
			}
			neg = sess.Negotiator()
			neg.PC = pc
			if err := neg.Accept(ctx); err != nil {
				_ = pc.Close()
				return fmt.Errorf("accept (planb): %w", err)
			}
		} else {
			return fmt.Errorf("accept: %w", err)
		}
	}
	if err := sess.WaitBridgeSCTP(ctx); err != nil {
		_ = pc.Close()
		return fmt.Errorf("wait sctp: %w", err)
	}
	fmt.Fprintln(os.Stderr, "bridge connected (sctp)")
	return nil
}
