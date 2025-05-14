// Copyright © 2025 Meroxa, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package box

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
)

var (
	ErrEmptyAccessToken = errors.New("access token is required")
	ErrBoxAPI           = errors.New("box API error")
	ErrNotAFolder       = errors.New("path must point to a directory")
	BaseURL             = "https://api.box.com"
	UploadBaseURL       = "https://upload.box.com"
)

type HTTPClient struct {
	accessToken string
	httpClient  *http.Client
}

func NewHTTPClient(accessToken string) (*HTTPClient, error) {
	if accessToken == "" {
		return nil, ErrEmptyAccessToken
	}

	return &HTTPClient{
		accessToken: accessToken,
		httpClient:  &http.Client{},
	}, nil
}

func (c *HTTPClient) Download(ctx context.Context, fileID string, rangeHeader string) (io.ReadCloser, error) {
	url := fmt.Sprintf("%s/2.0/files/%s/content", BaseURL, fileID)

	headers := make(map[string]string)
	if rangeHeader != "" {
		headers["Range"] = rangeHeader
	}

	resp, err := c.makeRequest(ctx, http.MethodGet, url, headers, nil)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		defer resp.Body.Close()
		return nil, parseError(resp)
	}

	return resp.Body, nil
}

func (c *HTTPClient) Upload(ctx context.Context, filename string, parentID int, content []byte) (*UploadResponse, error) {
	var requestBody bytes.Buffer
	writer := multipart.NewWriter(&requestBody)
	attributes := fmt.Sprintf(`{"name":"%s", "parent":{"id":"%d"}}`, filename, parentID)
	err := writer.WriteField("attributes", attributes)
	if err != nil {
		return nil, err
	}

	part, err := writer.CreateFormFile("file", filename)
	if err != nil {
		return nil, err
	}
	_, err = io.Copy(part, bytes.NewReader(content))
	if err != nil {
		return nil, err
	}

	err = writer.Close()
	if err != nil {
		return nil, err
	}

	headers := map[string]string{"Content-Type": writer.FormDataContentType()}
	resp, err := c.makeRequest(ctx, http.MethodPost, UploadBaseURL+"/api/2.0/files/content", headers, &requestBody)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	response := &UploadResponse{}
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return nil, fmt.Errorf("decode response failed: %w", err)
	}
	return response, nil
}

func (c *HTTPClient) Session(ctx context.Context, filename string, parentID int, filesize int64) (*SessionResponse, error) {
	request := SessionRequest{
		FolderID: parentID,
		FileName: filename,
		FileSize: filesize,
	}
	body, err := json.Marshal(request)
	if err != nil {
		return nil, err
	}

	headers := map[string]string{"Content-Type": "application/json"}
	resp, err := c.makeRequest(ctx, http.MethodPost, UploadBaseURL+"/api/2.0/files/upload_sessions", headers, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	response := &SessionResponse{}
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return nil, fmt.Errorf("decode response failed: %w", err)
	}
	return response, nil
}

func (c *HTTPClient) UploadChunk(ctx context.Context, chunk []byte, sessionID, digest, contentRange string) (*UploadChunkResponse, error) {
	headers := map[string]string{
		"Content-Range": contentRange,
		"Digest":        "SHA=" + digest,
	}

	url := fmt.Sprintf("%s/api/2.0/files/upload_sessions/%s", UploadBaseURL, sessionID)
	resp, err := c.makeRequest(ctx, http.MethodPut, url, headers, bytes.NewReader(chunk))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	response := &UploadChunkResponse{}
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return nil, fmt.Errorf("decode response failed: %w", err)
	}
	return response, nil
}

func (c *HTTPClient) CommitUpload(ctx context.Context, sessionID string, parts []Part) (*CommitUploadResponse, error) {
	url := fmt.Sprintf("%s/api/2.0/files/upload_sessions/%s/commit", UploadBaseURL, sessionID)
	request := &CommitUploadRequest{
		Parts: parts,
	}
	body, err := json.Marshal(request)
	if err != nil {
		return nil, err
	}

	headers := map[string]string{"Content-Type": "application/json"}
	resp, err := c.makeRequest(ctx, http.MethodPost, url, headers, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	response := &CommitUploadResponse{}
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return nil, fmt.Errorf("decode response failed: %w", err)
	}
	return response, nil
}

func (c *HTTPClient) ListFolderItems(ctx context.Context, folderID int, marker string, limit int) ([]Entry, string, bool, error) {
	url := fmt.Sprintf("%s/2.0/folders/%d/items?fields=parent,file_version,name,sequence_id,sha1,modified_at,size,extension&usemarker=true", BaseURL, folderID)
	if marker != "" {
		url = fmt.Sprintf("%s&marker=%s", url, marker)
	}
	if limit > 0 {
		url = fmt.Sprintf("%s&limit=%d", url, limit)
	}

	resp, err := c.makeRequest(ctx, http.MethodGet, url, nil, nil)
	if err != nil {
		return nil, "", false, err
	}
	defer resp.Body.Close()

	var result struct {
		Entries    []Entry `json:"entries"`
		NextMarker string  `json:"next_marker"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, "", false, fmt.Errorf("decode response failed: %w", err)
	}

	hasMore := result.NextMarker != ""
	return result.Entries, result.NextMarker, hasMore, nil
}

func (c *HTTPClient) VerifyFolder(ctx context.Context, id int) (bool, error) {
	url := fmt.Sprintf("%s/2.0/folders/%d", BaseURL, id)

	resp, err := c.makeRequest(ctx, http.MethodGet, url, nil, nil)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()

	var result struct {
		Type string `json:"type"`
		ID   string `json:"id"`
		Name string `json:"name"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return false, fmt.Errorf("decode response failed: %w", err)
	}

	if result.Type != "folder" {
		return false, ErrNotAFolder
	}

	return true, nil
}

func (c *HTTPClient) GetEvents(ctx context.Context, streamPosition int) ([]Event, int, error) {
	url := fmt.Sprintf("%s/2.0/events?stream_type=changes", BaseURL)

	if streamPosition != 0 {
		url = fmt.Sprintf("%s&stream_position=%d", url, streamPosition)
	}

	resp, err := c.makeRequest(ctx, http.MethodGet, url, nil, nil)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()

	var result struct {
		Entries            []Event `json:"entries"`
		NextStreamPosition int     `json:"next_stream_position"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, 0, fmt.Errorf("decode response failed: %w", err)
	}

	return result.Entries, result.NextStreamPosition, nil
}

func (c *HTTPClient) makeRequest(ctx context.Context, method, url string, headers map[string]string, reqBody io.Reader) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, url, reqBody)
	if err != nil {
		return nil, fmt.Errorf("create request failed: %w", err)
	}

	req.Header.Set("Authorization", "Bearer "+c.accessToken)

	for header, value := range headers {
		req.Header.Set(header, value)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		defer resp.Body.Close()
		return nil, parseError(resp)
	}

	return resp, nil
}

func parseError(resp *http.Response) error {
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read error response: %w", err)
	}

	var errResp struct {
		Type        string `json:"type"`
		Status      int    `json:"status"`
		Code        string `json:"code"`
		ContextInfo struct {
			Errors []struct {
				Reason  string `json:"reason"`
				Name    string `json:"name"`
				Message string `json:"message"`
			} `json:"errors"`
		} `json:"context_info"`
		Message string `json:"message"`
	}

	if err := json.Unmarshal(body, &errResp); err == nil && errResp.Type == "error" {
		var errors string
		for _, e := range errResp.ContextInfo.Errors {
			errors += fmt.Sprintf("[%s %s %s]", e.Reason, e.Name, e.Message)
		}
		return fmt.Errorf("%w: [status:%d] [code:%s] [%s] %s ", ErrBoxAPI, errResp.Status, errResp.Code, errResp.Message, errors)
	}

	return fmt.Errorf("%w (status %d): %s", ErrBoxAPI, resp.StatusCode, string(body))
}
