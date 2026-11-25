// Copyright Envoy AI Gateway Authors
// SPDX-License-Identifier: Apache-2.0
// The full text of the Apache license is available in the LICENSE file at
// the root of the repo.

package extproc

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"testing"

	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	extprocv3 "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/envoyproxy/ai-gateway/internal/apischema/openai"
	"github.com/envoyproxy/ai-gateway/internal/backendauth"
	"github.com/envoyproxy/ai-gateway/internal/filterapi"
	"github.com/envoyproxy/ai-gateway/internal/headermutator"
	"github.com/envoyproxy/ai-gateway/internal/internalapi"
	"github.com/envoyproxy/ai-gateway/internal/json"
	"github.com/envoyproxy/ai-gateway/internal/translator"
)

type postFilterAuthRecorder struct {
	bodies [][]byte
	paths  []string
}

func (r *postFilterAuthRecorder) Do(_ context.Context, headers map[string]string, body []byte) ([]internalapi.Header, error) {
	r.bodies = append(r.bodies, bytes.Clone(body))
	r.paths = append(r.paths, headers[":path"])
	return nil, nil
}

func TestUpstreamTranslationPreservesPostFilterContent(t *testing.T) {
	for _, tc := range []struct {
		name          string
		newTranslator func() translator.OpenAIChatCompletionTranslator
	}{
		{name: "OpenAI model override", newTranslator: func() translator.OpenAIChatCompletionTranslator {
			return translator.NewChatCompletionOpenAIToOpenAITranslator("", "provider-model")
		}},
		{name: "AWS Bedrock Converse", newTranslator: func() translator.OpenAIChatCompletionTranslator {
			return translator.NewChatCompletionOpenAIToAWSBedrockTranslator("anthropic.claude-sonnet-4-5")
		}},
		{name: "AWS Anthropic", newTranslator: func() translator.OpenAIChatCompletionTranslator {
			return translator.NewChatCompletionOpenAIToAWSAnthropicTranslator("bedrock-2023-05-31", "anthropic.claude-sonnet-4-5")
		}},
		{name: "GCP Anthropic", newTranslator: func() translator.OpenAIChatCompletionTranslator {
			return translator.NewChatCompletionOpenAIToGCPAnthropicTranslator("vertex-2023-10-16", "claude-sonnet-4-5")
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, streamed := range []bool{false, true} {
				t.Run(fmt.Sprintf("stream=%t", streamed), func(t *testing.T) {
					original := []byte(fmt.Sprintf(`{"model":"alias","messages":[{"role":"user","content":"original-private-value"}],"max_tokens":100,"stream":%t}`, streamed))
					filtered := []byte(fmt.Sprintf(`{"model":"alias","messages":[{"role":"user","content":"[[EMAIL_1]]"}],"max_tokens":100,"stream":%t}`, streamed))
					var parsed openai.ChatCompletionRequest
					require.NoError(t, json.Unmarshal(original, &parsed))
					parent := &chatCompletionProcessorRouterFilter{
						originalRequestBodyRaw: original,
						originalRequestBody:    &parsed,
						config:                 &filterapi.RuntimeConfig{},
						stream:                 streamed,
						upstreamFilterCount:    1,
					}
					processor := &chatCompletionProcessorUpstreamFilter{
						parent:            parent,
						translator:        tc.newTranslator(),
						metrics:           &mockMetrics{},
						logger:            slog.Default(),
						requestHeaders:    map[string]string{":path": "/v1/chat/completions"},
						modelNameOverride: "provider-model",
					}
					auth := &postFilterAuthRecorder{}
					processor.handler = auth
					_, err := processor.ProcessRequestHeaders(t.Context(), nil)
					require.NoError(t, err)
					require.Empty(t, auth.bodies, "payload auth waits for the filtered request body")
					response, err := processor.ProcessRequestBody(t.Context(), &extprocv3.HttpBody{Body: filtered, EndOfStream: true})
					require.NoError(t, err)
					body := response.GetRequestBody().GetResponse().GetBodyMutation().GetBody()
					require.Contains(t, string(body), "[[EMAIL_1]]")
					require.NotContains(t, string(body), "original-private-value")
					require.Equal(t, body, auth.bodies[0], "payload auth receives the final translated body")
					parent.upstreamFilterCount = 2
					retry := *processor
					retry.translator = tc.newTranslator()
					_, err = retry.ProcessRequestHeaders(t.Context(), nil)
					require.NoError(t, err)
					response, err = retry.ProcessRequestBody(t.Context(), &extprocv3.HttpBody{Body: body, EndOfStream: true})
					require.NoError(t, err)
					retriedBody := response.GetRequestBody().GetResponse().GetBodyMutation().GetBody()
					require.Contains(t, string(retriedBody), "[[EMAIL_1]]")
					require.NotContains(t, string(retriedBody), "original-private-value")
					require.Equal(t, retriedBody, auth.bodies[1])
					require.JSONEq(t, string(filtered), string(parent.filteredRequestBodyRaw))
				})
			}
		})
	}
}

func TestUpstreamTranslationRejectsInvalidPostFilterBody(t *testing.T) {
	original := []byte(`{"model":"alias","messages":[{"role":"user","content":"original-private-value"}]}`)
	var parsed openai.ChatCompletionRequest
	require.NoError(t, json.Unmarshal(original, &parsed))
	auth := &postFilterAuthRecorder{}
	processor := &chatCompletionProcessorUpstreamFilter{
		parent:         &chatCompletionProcessorRouterFilter{originalRequestBodyRaw: original, originalRequestBody: &parsed, config: &filterapi.RuntimeConfig{}},
		translator:     translator.NewChatCompletionOpenAIToOpenAITranslator("", "provider-model"),
		metrics:        &mockMetrics{},
		logger:         slog.Default(),
		requestHeaders: map[string]string{":path": "/v1/chat/completions"},
		handler:        auth,
	}
	response, err := processor.ProcessRequestBody(t.Context(), &extprocv3.HttpBody{Body: []byte("invalid JSON"), EndOfStream: true})
	require.NoError(t, err)
	require.NotNil(t, response.GetImmediateResponse())
	require.Equal(t, 400, int(response.GetImmediateResponse().GetStatus().GetCode()))
	require.Empty(t, auth.bodies)
}

func postFilterFixture(t *testing.T) (*chatCompletionProcessorUpstreamFilter, []byte) {
	t.Helper()
	original := []byte(`{"model":"alias","messages":[{"role":"user","content":"original-private-value"}],"max_tokens":100}`)
	filtered := []byte(`{"model":"alias","messages":[{"role":"user","content":"[[EMAIL_1]]"}],"max_tokens":100}`)
	var parsed openai.ChatCompletionRequest
	require.NoError(t, json.Unmarshal(original, &parsed))
	return &chatCompletionProcessorUpstreamFilter{
		parent:         &chatCompletionProcessorRouterFilter{originalRequestBodyRaw: original, originalRequestBody: &parsed, config: &filterapi.RuntimeConfig{}},
		translator:     translator.NewChatCompletionOpenAIToOpenAITranslator("", "provider-model"),
		metrics:        &mockMetrics{},
		logger:         slog.Default(),
		requestHeaders: map[string]string{":path": "/v1/chat/completions", ":method": "POST"},
	}, filtered
}

func TestUpstreamTranslationProcessesBufferedBodyWithTrailers(t *testing.T) {
	processor, filtered := postFilterFixture(t)
	auth := &postFilterAuthRecorder{}
	processor.handler = auth
	response, err := processor.ProcessRequestBody(t.Context(), &extprocv3.HttpBody{Body: filtered, EndOfStream: false})
	require.NoError(t, err)
	require.Contains(t, string(response.GetRequestBody().GetResponse().GetBodyMutation().GetBody()), "[[EMAIL_1]]")
	require.Len(t, auth.bodies, 1)
}

func TestUpstreamTranslationAuthenticatesFinalPath(t *testing.T) {
	processor, _ := postFilterFixture(t)
	processor.parent.originalRequestBody.Model = "claude-sonnet-4-5"
	processor.parent.originalModel = "claude-sonnet-4-5"
	original, err := json.Marshal(processor.parent.originalRequestBody)
	require.NoError(t, err)
	processor.parent.originalRequestBodyRaw = original
	processor.translator = translator.NewChatCompletionOpenAIToAWSAnthropicTranslator("bedrock-2023-05-31", "")
	auth := &postFilterAuthRecorder{}
	processor.handler = auth
	_, err = processor.ProcessRequestHeaders(t.Context(), nil)
	require.NoError(t, err)
	response, err := processor.ProcessRequestBody(t.Context(), &extprocv3.HttpBody{
		Body:        []byte(`{"model":"claude-haiku-4-5","messages":[{"role":"user","content":"[[EMAIL_1]]"}],"max_tokens":100}`),
		EndOfStream: true,
	})
	require.NoError(t, err)
	var finalPath string
	for _, header := range response.GetRequestBody().GetResponse().GetHeaderMutation().GetSetHeaders() {
		if header.GetHeader().GetKey() == ":path" {
			finalPath = string(header.GetHeader().GetRawValue())
		}
	}
	require.Contains(t, finalPath, "claude-haiku-4-5")
	require.Equal(t, finalPath, auth.paths[0])
	require.Equal(t, "claude-haiku-4-5", processor.metrics.(*mockMetrics).requestModel)
	require.Equal(t, "claude-sonnet-4-5", processor.metrics.(*mockMetrics).originalModel)
}

func TestUpstreamTranslationPreservesRequestModelOverride(t *testing.T) {
	processor, _ := postFilterFixture(t)
	processor.parent.originalModel = "client-model"
	processor.modelNameOverride = "provider-model"
	processor.requestHeaders[internalapi.ModelNameHeaderKeyDefault] = "provider-model"
	_, err := processor.ProcessRequestHeaders(t.Context(), nil)
	require.NoError(t, err)
	_, err = processor.ProcessRequestBody(t.Context(), &extprocv3.HttpBody{Body: []byte(`{"model":"filtered-model","messages":[{"role":"user","content":"[[EMAIL_1]]"}]}`), EndOfStream: true})
	require.NoError(t, err)
	require.Equal(t, "provider-model", processor.metrics.(*mockMetrics).requestModel)
	require.Equal(t, "client-model", processor.metrics.(*mockMetrics).originalModel)
}

func TestUpstreamTranslationRetainsEnvoyCredentialsBetweenCallbacks(t *testing.T) {
	for _, tc := range []struct {
		name     string
		metadata bool
		fallback bool
		clear    bool
		replace  bool
		want     string
	}{
		{name: "metadata without fallback", metadata: true, want: "Bearer metadata-fixture"},
		{name: "configured fallback", fallback: true, want: "Bearer configured-fixture"},
		{name: "missing credential fails closed"},
		{name: "body metadata revokes credential", metadata: true, clear: true},
		{name: "body metadata updates credential", metadata: true, replace: true, want: "Bearer metadata-next-fixture"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			processor, filtered := postFilterFixture(t)
			handler, err := backendauth.NewHandler(t.Context(), &filterapi.BackendAuth{
				APIKey:             &filterapi.APIKeyAuth{Key: "configured-fixture"},
				CredentialOverride: &filterapi.CredentialOverride{DynamicMetadataNamespace: "trusted-auth", DynamicMetadataKey: "credential", FallbackToConfigured: tc.fallback},
			})
			require.NoError(t, err)
			processor.handler = handler
			server := &Server{logger: slog.Default()}
			ctx := context.WithValue(t.Context(), loggerContextKey, slog.Default())
			headerRequest := &extprocv3.ProcessingRequest{Request: &extprocv3.ProcessingRequest_RequestHeaders{RequestHeaders: &extprocv3.HttpHeaders{Headers: &corev3.HeaderMap{}}}}
			if tc.metadata {
				headerRequest.MetadataContext = &corev3.Metadata{FilterMetadata: map[string]*structpb.Struct{
					"trusted-auth": {Fields: map[string]*structpb.Value{"credential": structpb.NewStringValue("metadata-fixture")}},
				}}
			}
			_, err = server.processMsg(ctx, processor, headerRequest, "fixture", true)
			require.NoError(t, err)
			bodyRequest := &extprocv3.ProcessingRequest{Request: &extprocv3.ProcessingRequest_RequestBody{
				RequestBody: &extprocv3.HttpBody{Body: filtered, EndOfStream: true},
			}}
			if tc.clear {
				bodyRequest.MetadataContext = &corev3.Metadata{}
			}
			if tc.replace {
				bodyRequest.MetadataContext = &corev3.Metadata{FilterMetadata: map[string]*structpb.Struct{
					"trusted-auth": {Fields: map[string]*structpb.Value{"credential": structpb.NewStringValue("metadata-next-fixture")}},
				}}
			}
			response, err := server.processMsg(ctx, processor, bodyRequest, "fixture", true)
			require.NoError(t, err)
			if tc.want == "" {
				require.Equal(t, 401, int(response.GetImmediateResponse().GetStatus().GetCode()))
				return
			}
			require.Nil(t, response.GetImmediateResponse())
			var authorization string
			for _, header := range response.GetRequestBody().GetResponse().GetHeaderMutation().GetSetHeaders() {
				if header.GetHeader().GetKey() == "Authorization" {
					authorization = string(header.GetHeader().GetRawValue())
				}
			}
			require.Equal(t, tc.want, authorization)
		})
	}
}

func TestUpstreamTranslationPreservesConfiguredHeaderPrecedence(t *testing.T) {
	processor, _ := postFilterFixture(t)
	processor.translator = &mockTranslator{
		t:                 t,
		expRequestBody:    processor.parent.originalRequestBody,
		retHeaderMutation: []internalapi.Header{{"x-provider-header", "translated"}, {"x-drop", "translated"}},
		retBodyMutation:   processor.parent.originalRequestBodyRaw,
	}
	processor.headerMutator = headermutator.NewHeaderMutator(&filterapi.HTTPHeaderMutation{
		Set:    []filterapi.HTTPHeader{{Name: "x-provider-header", Value: "configured"}},
		Remove: []string{"x-drop"},
	}, processor.requestHeaders)
	response, err := processor.ProcessRequestBody(t.Context(), &extprocv3.HttpBody{Body: processor.parent.originalRequestBodyRaw, EndOfStream: true})
	require.NoError(t, err)
	require.Equal(t, "configured", processor.requestHeaders["x-provider-header"])
	mutation := response.GetRequestBody().GetResponse().GetHeaderMutation()
	require.Contains(t, mutation.GetRemoveHeaders(), "x-drop")
	var value string
	for _, header := range mutation.GetSetHeaders() {
		require.NotEqual(t, "x-drop", header.GetHeader().GetKey())
		if header.GetHeader().GetKey() == "x-provider-header" {
			value = string(header.GetHeader().GetRawValue())
		}
	}
	require.Equal(t, "configured", value)
}

func TestUpstreamTranslationRejectsFilteredStreamModeChange(t *testing.T) {
	for _, originalStream := range []bool{false, true} {
		t.Run(fmt.Sprintf("original_stream=%t", originalStream), func(t *testing.T) {
			processor, _ := postFilterFixture(t)
			processor.parent.stream = originalStream
			processor.parent.originalRequestBody.Stream = originalStream
			auth := &postFilterAuthRecorder{}
			processor.handler = auth
			body := []byte(fmt.Sprintf(`{"model":"alias","messages":[{"role":"user","content":"[[EMAIL_1]]"}],"stream":%t}`, !originalStream))
			response, err := processor.ProcessRequestBody(t.Context(), &extprocv3.HttpBody{Body: body, EndOfStream: true})
			require.NoError(t, err)
			require.Equal(t, 400, int(response.GetImmediateResponse().GetStatus().GetCode()))
			require.Empty(t, auth.bodies)
		})
	}
}

func TestUpstreamTranslationForwardsParserCorrectedBytes(t *testing.T) {
	processor, _ := postFilterFixture(t)
	processor.parent.stream = true
	processor.parent.originalRequestBody.Stream = true
	processor.parent.config.GlobalRequestCosts = []filterapi.RuntimeGlobalRequestCost{{}}
	processor.translator = translator.NewChatCompletionOpenAIToOpenAITranslator("", "")
	body := []byte(`{"model":"alias","messages":[{"role":"user","content":"[[EMAIL_1]]"}],"stream":true}`)
	response, err := processor.ProcessRequestBody(t.Context(), &extprocv3.HttpBody{Body: body, EndOfStream: true})
	require.NoError(t, err)
	corrected := response.GetRequestBody().GetResponse().GetBodyMutation().GetBody()
	require.NotEmpty(t, corrected)
	require.Contains(t, string(corrected), "[[EMAIL_1]]")
	var parsed openai.ChatCompletionRequest
	require.NoError(t, json.Unmarshal(corrected, &parsed))
	require.True(t, parsed.StreamOptions.IncludeUsage)
}
