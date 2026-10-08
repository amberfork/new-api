package model

import (
	"testing"

	"github.com/QuantumNous/new-api/common"

	"github.com/stretchr/testify/require"
)

// TestFormatUserLogsStripsQuotaSaturation verifies the admin-only quota
// saturation marker (nested under other.admin_info) is removed for non-admin
// log views, since formatUserLogs strips the whole admin_info object.
func TestFormatUserLogsStripsQuotaSaturation(t *testing.T) {
	other := common.MapToJsonStr(map[string]interface{}{
		"model_price": 0.004,
		"admin_info": map[string]interface{}{
			"quota_saturation": map[string]interface{}{
				"op":      "QuotaFromDecimal",
				"kind":    "overflow",
				"clamped": common.MaxQuota,
			},
		},
	})
	logs := []*Log{{Other: other}}

	formatUserLogs(logs, 0)

	parsed, err := common.StrToMap(logs[0].Other)
	require.NoError(t, err)
	_, hasAdminInfo := parsed["admin_info"]
	require.False(t, hasAdminInfo, "admin_info (and nested quota_saturation) must be stripped for non-admin views")
	// Non-admin billing fields remain visible.
	require.Contains(t, parsed, "model_price")
}

// TestFormatUserLogsStripsChannelInfo verifies non-admin log views do not show
// which channel or upstream served a request (patch 0003).
func TestFormatUserLogsStripsChannelInfo(t *testing.T) {
	other := common.MapToJsonStr(map[string]interface{}{
		"error_code":   "bad_response_status_code",
		"status_code":  503,
		"channel_id":   52,
		"channel_name": "upstream-a-group-1",
		"channel_type": 1,
	})
	logs := []*Log{{ChannelId: 52, ChannelName: "upstream-a-group-1", UpstreamRequestId: "upstream-req-1", RequestId: "req-1", Other: other}}

	formatUserLogs(logs, 0)

	require.Zero(t, logs[0].ChannelId)
	require.Empty(t, logs[0].ChannelName)
	require.Empty(t, logs[0].UpstreamRequestId)
	require.Equal(t, "req-1", logs[0].RequestId, "our own request id stays visible")
	parsed, err := common.StrToMap(logs[0].Other)
	require.NoError(t, err)
	for _, k := range []string{"channel_id", "channel_name", "channel_type"} {
		require.NotContains(t, parsed, k)
	}
	require.Contains(t, parsed, "error_code")
	require.Contains(t, parsed, "status_code")
}
