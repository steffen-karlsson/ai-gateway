// Copyright Envoy AI Gateway Authors
// SPDX-License-Identifier: Apache-2.0
// The full text of the Apache license is available in the LICENSE file at
// the root of the repo.

package extproc

import (
	"bytes"
	"errors"
	"io"
	"log/slog"
	"mime"
	"mime/multipart"
	"testing"

	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	extprocv3 "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	"github.com/stretchr/testify/require"

	"github.com/envoyproxy/ai-gateway/internal/endpointspec"
	"github.com/envoyproxy/ai-gateway/internal/filterapi"
	"github.com/envoyproxy/ai-gateway/internal/headermutator"
	"github.com/envoyproxy/ai-gateway/internal/translator"
)

func TestUpstreamMultipartUsesFilteredBoundaryForFirstAttemptAndRetry(t *testing.T) {
	original, originalType := buildTestMultipartBody(t, map[string]string{"model": "whisper-1", "prompt": "original-private-value"}, "fixture.wav", []byte("fixture-audio"))
	filtered, filteredType := buildTestMultipartBody(t, map[string]string{"model": "whisper-1", "prompt": "[[EMAIL_1]]"}, "fixture.wav", []byte("fixture-audio"))
	require.NotEqual(t, originalType, filteredType)
	spec := endpointspec.TranscriptionEndpointSpec{}
	_, parsed, _, _, err := spec.ParseMultipartBody(original, originalType, false)
	require.NoError(t, err)
	for _, override := range []string{"provider-model", ""} {
		t.Run("override="+override, func(t *testing.T) {
			parent := &transcriptionProcessorRouterFilter{
				originalRequestBodyRaw: original, originalRequestBody: parsed,
				requestHeaders: map[string]string{"content-type": originalType},
				config:         &filterapi.RuntimeConfig{}, upstreamFilterCount: 1,
			}
			input, contentType := filtered, filteredType
			for attempt := 1; attempt <= 2; attempt++ {
				parent.upstreamFilterCount = attempt
				tr := translator.NewTranscriptionOpenAIToOpenAITranslator("", override)
				tr.(translator.ContentTypeSetter).SetContentType(originalType)
				processor := &transcriptionProcessorUpstreamFilter{
					parent: parent, translator: tr, metrics: &mockMetrics{}, logger: slog.Default(),
					requestHeaders: map[string]string{":path": "/v1/audio/transcriptions", "content-type": contentType},
				}
				processor.headerMutator = headermutator.NewHeaderMutator(nil, parent.requestHeaders)
				headers, err := processor.ProcessRequestHeaders(t.Context(), &corev3.HeaderMap{Headers: []*corev3.HeaderValue{{Key: "content-type", RawValue: []byte(contentType)}}})
				require.NoError(t, err)
				outputType := contentType
				for _, h := range headers.GetRequestHeaders().GetResponse().GetHeaderMutation().GetSetHeaders() {
					if h.GetHeader().GetKey() == "content-type" {
						outputType = string(h.GetHeader().GetRawValue())
					}
				}
				response, err := processor.ProcessRequestBody(t.Context(), &extprocv3.HttpBody{Body: input, EndOfStream: true})
				require.NoError(t, err)
				require.Nil(t, response.GetImmediateResponse())
				common := response.GetRequestBody().GetResponse()
				output := common.GetBodyMutation().GetBody()
				if output == nil {
					output = input
				}
				for _, h := range common.GetHeaderMutation().GetSetHeaders() {
					if h.GetHeader().GetKey() == "content-type" {
						outputType = string(h.GetHeader().GetRawValue())
					}
				}
				_, params, err := mime.ParseMediaType(outputType)
				require.NoError(t, err)
				reader := multipart.NewReader(bytes.NewReader(output), params["boundary"])
				fields := map[string]string{}
				for {
					part, err := reader.NextPart()
					if errors.Is(err, io.EOF) {
						break
					}
					require.NoError(t, err)
					data, err := io.ReadAll(part)
					require.NoError(t, err)
					fields[part.FormName()] = string(data)
				}
				require.Equal(t, "[[EMAIL_1]]", fields["prompt"])
				require.Equal(t, "fixture-audio", fields["file"])
				expectedModel := override
				if expectedModel == "" {
					expectedModel = "whisper-1"
				}
				require.Equal(t, expectedModel, fields["model"])
				input, contentType = output, outputType
			}
		})
	}
}

func TestUpstreamMultipartConfiguredContentTypePrecedence(t *testing.T) {
	original, originalType := buildTestMultipartBody(t, map[string]string{"model": "whisper-1", "prompt": "original"}, "fixture.wav", []byte("fixture-audio"))
	filtered, filteredType := buildTestMultipartBody(t, map[string]string{"model": "whisper-1", "prompt": "[[EMAIL_1]]"}, "fixture.wav", []byte("fixture-audio"))
	_, parsed, _, _, err := (endpointspec.TranscriptionEndpointSpec{}).ParseMultipartBody(original, originalType, false)
	require.NoError(t, err)
	for _, configuredType := range []string{"", "application/configured"} {
		t.Run("configured="+configuredType, func(t *testing.T) {
			for _, attempt := range []int{1, 2} {
				parent := &transcriptionProcessorRouterFilter{
					originalRequestBodyRaw: original, originalRequestBody: parsed,
					requestHeaders: map[string]string{"content-type": originalType},
					config:         &filterapi.RuntimeConfig{}, upstreamFilterCount: attempt,
				}
				if attempt == 2 {
					parent.filteredRequestBodyRaw, parent.filteredRequestContentType = filtered, filteredType
				}
				tr := translator.NewTranscriptionOpenAIToOpenAITranslator("", "")
				tr.(translator.ContentTypeSetter).SetContentType(originalType)
				config := &filterapi.HTTPHeaderMutation{Remove: []string{"content-type"}}
				if configuredType != "" {
					config.Set = []filterapi.HTTPHeader{{Name: "content-type", Value: configuredType}}
				}
				processor := &transcriptionProcessorUpstreamFilter{
					parent: parent, translator: tr, metrics: &mockMetrics{}, logger: slog.Default(),
					requestHeaders: map[string]string{"content-type": filteredType},
					headerMutator:  headermutator.NewHeaderMutator(config, parent.requestHeaders),
				}
				headers, err := processor.ProcessRequestHeaders(t.Context(), &corev3.HeaderMap{Headers: []*corev3.HeaderValue{{Key: "content-type", RawValue: []byte(filteredType)}}})
				require.NoError(t, err)
				response, err := processor.ProcessRequestBody(t.Context(), &extprocv3.HttpBody{Body: filtered, EndOfStream: true})
				require.NoError(t, err)
				require.Nil(t, response.GetImmediateResponse())
				outgoingType := filteredType
				for _, mutation := range []*extprocv3.HeaderMutation{headers.GetRequestHeaders().GetResponse().GetHeaderMutation(), response.GetRequestBody().GetResponse().GetHeaderMutation()} {
					for _, key := range mutation.GetRemoveHeaders() {
						if key == "content-type" {
							outgoingType = ""
						}
					}
					for _, header := range mutation.GetSetHeaders() {
						if header.GetHeader().GetKey() == "content-type" {
							outgoingType = string(header.GetHeader().GetRawValue())
						}
					}
				}
				require.Equal(t, configuredType, outgoingType, "attempt %d", attempt)
			}
		})
	}
}
