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

import "time"

// UploadResponse represents the response returned after uploading a file to Box.
// It includes metadata about the uploaded file(s).
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

// SessionRequest represents the payload to initiate a new chunked upload session.
type SessionRequest struct {
	FolderID int    `json:"folder_id,omitempty"`
	FileName string `json:"file_name,omitempty"`
	FileSize int64  `json:"file_size"`
}

// SessionResponse contains details of a chunked upload session.
type SessionResponse struct {
	ID                string `json:"id"`
	Type              string `json:"type"`
	NumPartsProcessed int    `json:"num_parts_processed"`
	PartSize          int    `json:"part_size"`
	SessionExpiresAt  string `json:"session_expires_at"`
	TotalParts        int    `json:"total_parts"`
}

// UploadChunkResponse contains information about an individual chunk uploaded as part of a session.
type UploadChunkResponse struct {
	Part Part `json:"part"`
}

// CommitUploadRequest defines the payload to finalize a chunked upload session.
type CommitUploadRequest struct {
	Parts []Part `json:"parts"`
}

// Part represents a chunk of a file uploaded during a chunked session.
type Part struct {
	PartID string `json:"part_id"`
	Offset int    `json:"offset"`
	Size   int    `json:"size"`
	Sha1   string `json:"sha1"`
}

// CommitUploadResponse contains the result of a committed upload session,
// listing the uploaded file(s).
type CommitUploadResponse struct {
	Entries    []Entry `json:"entries"`
	TotalCount int     `json:"total_count"`
}

// Entry represents metadata about a file or folder in Box.
type Entry struct {
	Etag        string `json:"etag"`
	ID          string `json:"id"`
	Type        string `json:"type"`
	FileVersion struct {
		ID   string `json:"id"`
		Type string `json:"type"`
		Sha1 string `json:"sha1"`
	} `json:"file_version"`
	Parent struct {
		ID   string `json:"id"`
		Name string `json:"name"`
	} `json:"parent"`
	PathCollection PathCollection `json:"path_collection"`
	Name           string         `json:"name"`
	SequenceID     string         `json:"sequence_id"`
	Sha1           string         `json:"sha1"`
	CreatedAt      time.Time      `json:"created_at"`
	ModifiedAt     time.Time      `json:"modified_at"`
	Size           int            `json:"size"`
	Extension      string         `json:"extension"`
}

// PathCollectionEntry represents a single entry in the path collection
type PathCollectionEntry struct {
	Type       string  `json:"type"`
	ID         string  `json:"id"`
	SequenceID *string `json:"sequence_id"`
	Etag       *string `json:"etag"`
	Name       string  `json:"name"`
}

// PathCollection represents the collection of entries
type PathCollection struct {
	TotalCount int                   `json:"total_count"`
	Entries    []PathCollectionEntry `json:"entries"`
}

// Event represents a change event in Box, such as file creation or modification.
type Event struct {
	Type      string    `json:"event_type"`
	EventID   string    `json:"event_id"`
	CreatedAt time.Time `json:"created_at"`
	Source    Entry     `json:"source"`
}
