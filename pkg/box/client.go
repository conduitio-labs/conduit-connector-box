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

func (c *HTTPClient) Download(_ context.Context) ([]byte, error) {
	return nil, nil
}

func (c *HTTPClient) Upload(ctx context.Context, filename, parentID, fileID string, content []byte) (*UploadResponse, error) {
	url := fmt.Sprintf("%s/api/2.0/files/content", UploadBaseURL)
	if fileID != "" {
		url = fmt.Sprintf("%s/api/2.0/files/%s/content", UploadBaseURL, fileID)
	}

	var requestBody bytes.Buffer
	writer := multipart.NewWriter(&requestBody)
	attributes := fmt.Sprintf(`{"name":"%s", "parent":{"id":"%s"}}`, filename, parentID)
	err := writer.WriteField("attributes", attributes)
	if err != nil {
		return nil, fmt.Errorf("error multipart write field: %w", err)
	}

	part, err := writer.CreateFormFile("file", filename)
	if err != nil {
		return nil, fmt.Errorf("error creating form file: %w", err)
	}
	_, err = io.Copy(part, bytes.NewReader(content))
	if err != nil {
		return nil, fmt.Errorf("error copying content: %w", err)
	}

	err = writer.Close()
	if err != nil {
		return nil, fmt.Errorf("error closing multipart writer: %w", err)
	}

	headers := map[string]string{"Content-Type": writer.FormDataContentType()}
	resp, err := c.makeRequest(ctx, http.MethodPost, url, headers, &requestBody)
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

func (c *HTTPClient) Session(ctx context.Context, filename, parentID, fileID string, filesize int64) (*SessionResponse, error) {
	request := SessionRequest{}
	var url string

	if fileID != "" {
		request.FileSize = filesize
		url = fmt.Sprintf("%s/api/2.0/files/%s/upload_sessions", UploadBaseURL, fileID)
	} else {
		request.FolderID = parentID
		request.FileName = filename
		request.FileSize = filesize
		url = fmt.Sprintf("%s/api/2.0/files/upload_sessions", UploadBaseURL)
	}

	body, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("error marshalling session request: %w", err)
	}

	headers := map[string]string{"Content-Type": "application/json"}
	resp, err := c.makeRequest(ctx, http.MethodPost, url, headers, bytes.NewReader(body))
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
		"Content-Type":  "application/octet-stream",
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

func (c *HTTPClient) CommitUpload(ctx context.Context, sessionID, digest string, parts []Part) (*CommitUploadResponse, error) {
	url := fmt.Sprintf("%s/api/2.0/files/upload_sessions/%s/commit", UploadBaseURL, sessionID)
	request := &CommitUploadRequest{
		Parts: parts,
	}
	body, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("error marshalling commit request: %w", err)
	}

	headers := map[string]string{
		"Content-Type": "application/json",
		"Digest":       "SHA=" + digest,
	}
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

func (c *HTTPClient) Delete(ctx context.Context, fileID string) error {
	url := fmt.Sprintf("%s/api/2.0/files/%s", BaseURL, fileID)
	headers := map[string]string{"Content-Type": "application/json"}
	resp, err := c.makeRequest(ctx, http.MethodDelete, url, headers, nil)
	if err != nil {
		return err
	}
	resp.Body.Close()
	return nil
}

func (c *HTTPClient) ListFolderItems(ctx context.Context, folderID, marker string, limit int) ([]Entry, string, bool, error) {
	url := fmt.Sprintf("%s/2.0/folders/%s/items?fields=parent,file_version,name,sequence_id,sha1,modified_at,size,extension&usemarker=true", BaseURL, folderID)
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

func (c *HTTPClient) Close() {
	c.httpClient.CloseIdleConnections()
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

type UploadResponse struct {
	TotalCount int `json:"total_count"`
	Entries    []struct {
		Type        string `json:"type"`
		ID          string `json:"id"`
		FileVersion struct {
			Type string `json:"type"`
			ID   string `json:"id"`
			Sha1 string `json:"sha1"`
		} `json:"file_version"`
		SequenceID string `json:"sequence_id"`
		Name       string `json:"name"`
		Size       int64  `json:"size"`
		CreatedAt  string `json:"created_at"`
		ModifiedAt string `json:"modified_at"`
	} `json:"entries"`
}

type SessionRequest struct {
	FolderID string `json:"folder_id,omitempty"`
	FileName string `json:"file_name,omitempty"`
	FileSize int64  `json:"file_size"`
}

type SessionResponse struct {
	ID                string `json:"id"`
	Type              string `json:"type"`
	NumPartsProcessed int    `json:"num_parts_processed"`
	PartSize          int    `json:"part_size"`
	SessionExpiresAt  string `json:"session_expires_at"`
	TotalParts        int    `json:"total_parts"`
}

type UploadChunkResponse struct {
	Part Part `json:"part"`
}

type CommitUploadRequest struct {
	Parts []Part `json:"parts"`
}

type Part struct {
	PartID string `json:"part_id"`
	Offset int    `json:"offset"`
	Size   int    `json:"size"`
	Sha1   string `json:"sha1"`
}

type CommitUploadResponse struct {
	Entries    []Entry `json:"entries"`
	TotalCount int     `json:"total_count"`
}

type Entry struct {
	Etag        string `json:"etag"`
	ID          string `json:"id"`
	Type        string `json:"type"`
	FileVersion struct {
		ID   string `json:"id"`
		Type string `json:"type"`
		Sha1 string `json:"sha1"`
	} `json:"file_version"`
	Name              string `json:"name"`
	SequenceID        string `json:"sequence_id"`
	Sha1              string `json:"sha1"`
	ContentCreatedAt  string `json:"content_created_at"`
	ContentModifiedAt string `json:"content_modified_at"`
	CreatedAt         string `json:"created_at"`
	ModifiedAt        string `json:"modified_at"`
	Size              int    `json:"size"`
	Extension         string `json:"extension"`
}
