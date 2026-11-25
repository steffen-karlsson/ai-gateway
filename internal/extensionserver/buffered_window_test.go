// Copyright Envoy AI Gateway Authors
// SPDX-License-Identifier: Apache-2.0
// The full text of the Apache license is available in the LICENSE file at
// the root of the repo.

package extensionserver

import (
	"testing"

	egextension "github.com/envoyproxy/gateway/proto/extension"
	httpv3 "github.com/envoyproxy/go-control-plane/envoy/extensions/upstreams/http/v3"
	"github.com/stretchr/testify/require"
)

func TestBufferedStreamWindowCoversSupportedBodyLimit(t *testing.T) {
	server := &Server{k8sClient: newFakeClient()}
	response, err := server.PostTranslateModify(t.Context(), &egextension.PostTranslateModifyRequest{})
	require.NoError(t, err)
	for _, cluster := range response.Clusters {
		if cluster.Name == extProcUDSClusterName {
			options := &httpv3.HttpProtocolOptions{}
			require.NoError(t, cluster.TypedExtensionProtocolOptions["envoy.extensions.upstreams.http.v3.HttpProtocolOptions"].UnmarshalTo(options))
			window := options.GetExplicitHttpConfig().GetHttp2ProtocolOptions().GetInitialStreamWindowSize().GetValue()
			require.GreaterOrEqual(t, window, uint32(48*1024*1024), "cover the deployment chart's maximum admitted body")
			return
		}
	}
	t.Fatal("external processor UDS cluster not found")
}
