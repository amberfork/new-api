package common

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/iotest"

	"github.com/QuantumNous/new-api/relaykit/types"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

// Patch 0004.

// smallReads hands out at most 4 KiB per Read, like a stream scanner does.
type smallReads struct{ r io.Reader }

func (s smallReads) Read(p []byte) (int, error) {
	if len(p) > 4096 {
		p = p[:4096]
	}
	return s.r.Read(p)
}

func watchUpstreamModelBody(t *testing.T, format types.RelayFormat, status int, contentType string, body io.Reader) (*gin.Context, *http.Response) {
	t.Helper()
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	info := &RelayInfo{RelayFormat: format, ChannelMeta: &ChannelMeta{UpstreamModelName: "asked-model"}}
	resp := &http.Response{StatusCode: status, Header: http.Header{"Content-Type": {contentType}}, Body: io.NopCloser(body)}
	WatchUpstreamModel(c, info, resp)
	return c, resp
}

func TestWatchUpstreamModelReadsModelAndKeepsBytes(t *testing.T) {
	cases := []struct {
		name, contentType, body, want string
		format                        types.RelayFormat
	}{
		{"chat stream", "text/event-stream", "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"model\":\"served-model\",\"choices\":[{\"delta\":{\"content\":\"hi \\\"model\\\":\\\"x\\\"\"}}]}\n\ndata: {\"model\":\"later-model\"}\n\ndata: [DONE]\n\n", "served-model", types.RelayFormatOpenAI},
		{"chat stream, first chunk without model", "text/event-stream; charset=utf-8", ": ping\n\ndata: {\"choices\":[]}\r\n\r\ndata: {\"model\":\"\"}\n\ndata: {\"model\":\"served-model\"}\n\n", "served-model", types.RelayFormatOpenAI},
		{"responses stream", "text/event-stream", "event: response.created\ndata: {\"type\":\"response.created\",\"response\":{\"id\":\"r1\",\"instructions\":\"" + strings.Repeat("x", 40000) + "\",\"model\":\"served-model\"}}\n\nevent: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"hi\"}\n\n", "served-model", types.RelayFormatOpenAIResponses},
		{"claude stream", "text/event-stream", "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"m1\",\"type\":\"message\",\"role\":\"assistant\",\"model\":\"served-model\",\"content\":[]}}\n\nevent: ping\ndata: {\"type\": \"ping\"}\n\n", "served-model", types.RelayFormatClaude},
		{"gemini stream", "text/event-stream", "data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"hi\"}]}}],\"modelVersion\":\"served-model\"}\n\n", "served-model", types.RelayFormatGemini},
		{"stream ends without newline", "text/event-stream", "data: {\"model\":\"served-model\"}", "served-model", types.RelayFormatOpenAI},
		{"chat", "application/json", "{\"id\":\"c1\",\"object\":\"chat.completion\",\"model\":\"served-model\",\"choices\":[]}", "served-model", types.RelayFormatOpenAI},
		{"responses", "application/json", "{\"id\":\"r1\",\"object\":\"response\",\"instructions\":\"x\",\"model\":\"served-model\",\"output\":[]}", "served-model", types.RelayFormatOpenAIResponses},
		{"claude", "application/json", "{\n  \"id\": \"m1\",\n  \"type\": \"message\",\n  \"model\": \"served-model\"\n}\n", "served-model", types.RelayFormatClaude},
		{"gemini", "application/json", "{\"candidates\":[],\"usageMetadata\":{\"promptTokenCount\":1},\"modelVersion\":\"served-model\",\"responseId\":\"x\"}", "served-model", types.RelayFormatGemini},
		{"chat as claude request", "application/json", "{\"model\":\"served-model\",\"choices\":[]}", "served-model", types.RelayFormatClaude},
		{"not json", "application/json", "<html>bad gateway</html>", "", types.RelayFormatOpenAI},
		{"truncated json without model", "application/json", "{\"id\":\"c1\",\"choi", "", types.RelayFormatOpenAI},
		{"model is not a string", "application/json", "{\"model\":{\"id\":\"x\"}}", "", types.RelayFormatOpenAI},
		{"stream without model", "text/event-stream", "data: {\"choices\":[]}\n\ndata: not-json\n\ndata: [DONE]\n\n", "", types.RelayFormatOpenAI},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// One byte per Read: every chunk boundary is exercised.
			c, resp := watchUpstreamModelBody(t, tc.format, http.StatusOK, tc.contentType, iotest.OneByteReader(strings.NewReader(tc.body)))
			got, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			require.NoError(t, resp.Body.Close())
			require.Equal(t, tc.body, string(got), "body must pass through unchanged")
			m := UpstreamModelForLog(c)
			if tc.want == "" {
				require.Nil(t, m)
				return
			}
			require.Equal(t, map[string]interface{}{"sent": "asked-model", "returned": tc.want}, m)
		})
	}
}

func TestWatchUpstreamModelStreamKnownBeforeEnd(t *testing.T) {
	pr, pw := io.Pipe()
	c, resp := watchUpstreamModelBody(t, types.RelayFormatOpenAI, http.StatusOK, "text/event-stream", pr)
	go func() { _, _ = pw.Write([]byte("data: {\"model\":\"served-model\"}\n\n")) }()
	buf := make([]byte, 64)
	_, err := resp.Body.Read(buf)
	require.NoError(t, err)
	require.Equal(t, "served-model", UpstreamModelForLog(c)["returned"], "the first event is enough; the stream is still open")
	_ = pw.Close()
}

func TestWatchUpstreamModelGivesUpOnLargeBodies(t *testing.T) {
	big := "{\"data\":[{\"b64_json\":\"" + strings.Repeat("A", upstreamModelMaxBody) + "\"}],\"model\":\"served-model\"}"
	c, resp := watchUpstreamModelBody(t, types.RelayFormatGemini, http.StatusOK, "application/json", strings.NewReader(big))
	got, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Len(t, got, len(big))
	require.Nil(t, UpstreamModelForLog(c))

	stream := "data: {\"choices\":[]}\n\n"
	// The model shows up only after 2 MiB of events without one.
	stream = strings.Repeat(stream, 2*upstreamModelMaxStream/len(stream)) + "data: {\"model\":\"served-model\"}\n\n"
	c, resp = watchUpstreamModelBody(t, types.RelayFormatOpenAI, http.StatusOK, "text/event-stream", smallReads{strings.NewReader(stream)})
	got, err = io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Len(t, got, len(stream))
	require.Nil(t, UpstreamModelForLog(c))
}

func TestWatchUpstreamModelSkips(t *testing.T) {
	body := "{\"model\":\"served-model\"}"
	for name, tc := range map[string]struct {
		format types.RelayFormat
		status int
	}{
		"non-2xx":      {types.RelayFormatOpenAI, http.StatusServiceUnavailable},
		"image format": {types.RelayFormatOpenAIImage, http.StatusOK},
		"task format":  {types.RelayFormatTask, http.StatusOK},
	} {
		t.Run(name, func(t *testing.T) {
			src := io.NopCloser(strings.NewReader(body))
			c, _ := gin.CreateTestContext(httptest.NewRecorder())
			resp := &http.Response{StatusCode: tc.status, Header: http.Header{}, Body: src}
			WatchUpstreamModel(c, &RelayInfo{RelayFormat: tc.format, ChannelMeta: &ChannelMeta{UpstreamModelName: "asked-model"}}, resp)
			require.Equal(t, src, resp.Body, "body is not wrapped")
			_, _ = io.ReadAll(resp.Body)
			require.Nil(t, UpstreamModelForLog(c))
		})
	}
	require.Nil(t, UpstreamModelForLog(nil))
	WatchUpstreamModel(nil, nil, nil)
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	WatchUpstreamModel(c, &RelayInfo{RelayFormat: types.RelayFormatOpenAI}, &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(body))})
	require.Nil(t, UpstreamModelForLog(c), "no channel meta: nothing recorded, no panic")
}

func TestWatchUpstreamModelRetryResets(t *testing.T) {
	c, resp := watchUpstreamModelBody(t, types.RelayFormatOpenAI, http.StatusOK, "application/json", strings.NewReader("{\"model\":\"first-model\"}"))
	_, _ = io.ReadAll(resp.Body)
	require.Equal(t, "first-model", UpstreamModelForLog(c)["returned"])
	// Next attempt (another channel) answers 500: the earlier record must not stay.
	WatchUpstreamModel(c, &RelayInfo{RelayFormat: types.RelayFormatOpenAI, ChannelMeta: &ChannelMeta{}}, &http.Response{StatusCode: 500, Body: io.NopCloser(strings.NewReader(""))})
	require.Nil(t, UpstreamModelForLog(c))
}

func TestWatchUpstreamModelPassesReadErrors(t *testing.T) {
	boom := errors.New("connection reset")
	c, resp := watchUpstreamModelBody(t, types.RelayFormatOpenAI, http.StatusOK, "application/json",
		io.MultiReader(strings.NewReader("{\"model\":\"served-model\",\"cho"), iotest.ErrReader(boom)))
	got, err := io.ReadAll(resp.Body)
	require.ErrorIs(t, err, boom)
	require.Equal(t, "{\"model\":\"served-model\",\"cho", string(got))
	// The model was already in the part we got.
	require.Equal(t, "served-model", UpstreamModelForLog(c)["returned"])
}
