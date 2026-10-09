package common

// Patch 0004: record which model the upstream says it served.
//
// An upstream can answer a request for one model with another (asked for
// gpt-5.3-codex, served gpt-5.5) and say so in its own response. To catch that
// from real traffic, the model field of the upstream response is read while the
// relay reads the body, and the consume log gets it under other.admin_info,
// which formatUserLogs strips for non-admin viewers.
//
// Strictly passive: the body bytes go through unchanged, nothing here returns an
// error, and a body we cannot parse records nothing. Streams are only looked at
// until the first event that names a model; non-stream bodies are kept (up to
// upstreamModelMaxBody) and parsed once, at EOF or Close.

import (
	"bytes"
	"io"
	"net/http"
	"strings"
	"sync"

	"github.com/QuantumNous/new-api/relaykit/types"

	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"
)

const (
	upstreamModelContextKey = "sr_upstream_model_watch"
	upstreamModelMaxBody    = 4 << 20 // larger non-stream bodies (inline images) are not parsed
	upstreamModelMaxStream  = 1 << 20 // a stream that names no model in its first 1 MiB is not looked at further
	upstreamModelMaxLen     = 200
)

// Tried in order on each JSON document: chat / completions and non-stream
// Responses and Claude (model), streamed Responses events (response.model),
// Claude message_start (message.model), Gemini (modelVersion).
var upstreamModelPaths = []string{"model", "response.model", "message.model", "modelVersion"}

type upstreamModelWatch struct {
	body   io.ReadCloser
	sent   string
	stream bool

	mu       sync.Mutex
	done     bool
	seen     int
	buf      []byte
	returned string
}

// WatchUpstreamModel wraps a successful upstream response body so the model the
// upstream reports is recorded. Every upstream attempt resets the record, so
// after retries it belongs to the response that was relayed.
func WatchUpstreamModel(c *gin.Context, info *RelayInfo, resp *http.Response) {
	if c == nil {
		return
	}
	var w *upstreamModelWatch
	if info != nil && info.ChannelMeta != nil && resp != nil && resp.Body != nil &&
		resp.StatusCode/100 == 2 && watchesUpstreamModel(info.RelayFormat) {
		w = &upstreamModelWatch{
			body:   resp.Body,
			sent:   info.UpstreamModelName,
			stream: strings.HasPrefix(resp.Header.Get("Content-Type"), "text/event-stream"),
		}
		resp.Body = w
	}
	c.Set(upstreamModelContextKey, w)
}

// UpstreamModelForLog returns {"sent": model sent upstream, "returned": model
// the upstream reported}, or nil when no model could be read.
func UpstreamModelForLog(c *gin.Context) map[string]interface{} {
	if c == nil {
		return nil
	}
	v, _ := c.Get(upstreamModelContextKey)
	w, _ := v.(*upstreamModelWatch)
	if w == nil {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.returned == "" {
		return nil
	}
	return map[string]interface{}{"sent": w.sent, "returned": w.returned}
}

func watchesUpstreamModel(f types.RelayFormat) bool {
	switch f {
	case types.RelayFormatOpenAI, types.RelayFormatOpenAIResponses, types.RelayFormatClaude, types.RelayFormatGemini:
		return true
	}
	return false
}

func (w *upstreamModelWatch) Read(p []byte) (int, error) {
	n, err := w.body.Read(p)
	w.observe(p[:n], err != nil)
	return n, err
}

func (w *upstreamModelWatch) Close() error {
	w.observe(nil, true)
	return w.body.Close()
}

func (w *upstreamModelWatch) observe(chunk []byte, end bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.done {
		return
	}
	if w.stream && w.seen > upstreamModelMaxStream {
		w.finish("")
		return
	}
	w.seen += len(chunk)
	if !w.stream {
		if w.seen > upstreamModelMaxBody {
			w.finish("")
			return
		}
		w.buf = append(w.buf, chunk...)
		if end {
			w.finish(upstreamModelFromJSON(w.buf))
		}
		return
	}
	w.buf = append(w.buf, chunk...)
	for {
		i := bytes.IndexByte(w.buf, '\n')
		if i < 0 {
			break
		}
		line := w.buf[:i]
		w.buf = w.buf[i+1:]
		if m := upstreamModelFromSSELine(line); m != "" {
			w.finish(m)
			return
		}
	}
	if end {
		w.finish(upstreamModelFromSSELine(w.buf))
	}
}

func (w *upstreamModelWatch) finish(model string) {
	w.done = true
	w.buf = nil
	w.returned = model
}

func upstreamModelFromSSELine(line []byte) string {
	line = bytes.TrimSpace(line)
	if !bytes.HasPrefix(line, []byte("data:")) {
		return ""
	}
	return upstreamModelFromJSON(bytes.TrimSpace(line[len("data:"):]))
}

func upstreamModelFromJSON(doc []byte) string {
	if !bytes.HasPrefix(bytes.TrimSpace(doc), []byte("{")) {
		return ""
	}
	for _, path := range upstreamModelPaths {
		r := gjson.GetBytes(doc, path)
		if r.Type == gjson.String && r.Str != "" && len(r.Str) <= upstreamModelMaxLen {
			return r.Str
		}
	}
	return ""
}
