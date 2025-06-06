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
	"context"
)

type Box interface {
	// Download returns the contents of a file in binary format.
	// Docs: https://developer.box.com/reference/get-files-id-content
	Download(ctx context.Context) ([]byte, error)

	// Upload uploads a file to box folder.
	// Docs: https://developer.box.com/reference/post-files-content
	Upload(ctx context.Context, filename, parentID, fileID string, content []byte) (*UploadResponse, error)

	// Session starts a multi-chunk upload session for large files.
	// Docs: https://developer.box.com/reference/post-files-upload-sessions
	Session(ctx context.Context, filename, parentID, fileID string, filesize int64) (*SessionResponse, error)

	// UploadChunk uploads a chunk to an existing upload session.
	// Docs: https://developer.box.com/reference/put-files-upload-sessions-id
	UploadChunk(ctx context.Context, chunk []byte, sessionID, digest, contentRange string) (*UploadChunkResponse, error)

	// CommitUpload finishes a multi-chunk upload session and creates the file.
	// Docs: https://developer.box.com/reference/post-files-upload-sessions-id-commit
	CommitUpload(ctx context.Context, sessionID, digest string, parts []Part) (*CommitUploadResponse, error)

	// Delete deletes a file with given fileID.
	// Docs: https://developer.box.com/reference/delete-files-id
	Delete(ctx context.Context, fileID string) error

	// ListFolderItems returns the items (files/folders) within a Box folder.
	// Docs: https://developer.box.com/reference/get-folders-id-items
	ListFolderItems(ctx context.Context, folderID, marker string, limit int) ([]Entry, string, bool, error)

	// Close closes the HTTPClient
	Close()
}
