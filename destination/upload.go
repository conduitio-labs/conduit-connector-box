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

package destination

import (
	"context"
	"crypto/sha1" //nolint:gosec // box expects sha1 hash in api headers.
	"encoding/base64"
	"fmt"
	"strconv"

	"github.com/conduitio-labs/conduit-connector-box/pkg/box"
	"github.com/conduitio/conduit-commons/opencdc"
)

// uploadFile uploads a new or existing file to the remote box directory.
func (d *Destination) uploadFile(ctx context.Context, r opencdc.Record) error {
	filename, ok := r.Metadata[opencdc.MetadataFileName]
	if !ok {
		return NewInvalidFileError("missing filename")
	}
	size, ok := r.Metadata[opencdc.MetadataFileSize]
	if !ok {
		return NewInvalidFileError("missing file size")
	}
	filesize, err := strconv.ParseInt(size, 10, 64)
	if err != nil {
		return NewInvalidFileError("invalid file size" + err.Error())
	}

	var fileID string
	if r.Operation == opencdc.OperationUpdate {
		var err error
		fmt.Println("getting id by filename ---- ", filename)
		fileID, err = d.getFileIDByFilename(ctx, filename)
		if err != nil {
			return err
		}
		fmt.Println("got id ---- ", fileID)
	}

	response, err := d.client.Upload(ctx, filename, d.config.ParentID, fileID, r.Payload.After.Bytes())
	if err != nil {
		return fmt.Errorf("error uploading file: %w", err)
	}

	if response.Entries[0].Size != filesize {
		return NewInvalidFileError("filesize mismatch")
	}

	return nil
}

// handleFileChunk handles the chunked record to upload to remote box directory.
func (d *Destination) handleFileChunk(ctx context.Context, r opencdc.Record) error {
	metaData, err := d.extractMetadata(r)
	if err != nil {
		return err
	}

	if r.Operation == opencdc.OperationUpdate {
		metaData.fileID, err = d.getFileIDByFilename(ctx, metaData.filename)
		if err != nil {
			return err
		}
	}

	if metaData.filesize < int64(minChunkUploadSize) {
		return d.cachedUpload(ctx, metaData, r.Payload.After.Bytes())
	}

	return d.handleChunkUpload(ctx, metaData, r)
}

func (d *Destination) handleChunkUpload(ctx context.Context, metaData metadata, r opencdc.Record) error {
	sess, ok := d.sessions[metaData.hash]
	if !ok && metaData.index == 1 {
		if err := d.createSession(ctx, metaData); err != nil {
			return err
		}
		sess = d.sessions[metaData.hash]
	}

	switch sess.partSize {
	case maxRecordSize:
		return d.uploadStandardChunk(ctx, metaData, r.Payload.After.Bytes(), sess)
	default:
		return d.uploadParts(ctx, metaData, r.Payload.After.Bytes())
	}
}

func (d *Destination) uploadStandardChunk(ctx context.Context, metaData metadata, chunk []byte, sess session) error {
	shaSum := sha1.Sum(chunk) //nolint:gosec // box expects sha1 hash in api headers.
	shaB64 := base64.StdEncoding.EncodeToString(shaSum[:])
	start := sess.partsProcessed * sess.partSize
	end := start + sess.partSize - 1
	contentRange := fmt.Sprintf("bytes %d-%d/%d", start, end, metaData.filesize)

	resp, err := d.client.UploadChunk(ctx, d.files[metaData.hash], sess.sessionID, shaB64, contentRange)
	if err != nil {
		return fmt.Errorf("error uploading chunk: %w", err)
	}

	sess.partsProcessed++
	sess.parts = append(sess.parts, resp.Part)

	if _, err := sess.hasher.Write(d.files[metaData.hash]); err != nil {
		return fmt.Errorf("error hasher write: %w", err)
	}

	if metaData.index == metaData.totalChunks {
		if err := d.finalizeUpload(ctx, metaData.hash, sess); err != nil {
			return err
		}
	}

	return nil
}

func (d *Destination) finalizeUpload(ctx context.Context, hash string, sess session) error {
	if _, err := d.client.CommitUpload(ctx, sess.sessionID, hash, sess.parts); err != nil {
		return fmt.Errorf("error committing chunk upload: %w", err)
	}
	delete(d.sessions, hash)
	delete(d.files, hash)
	return nil
}

// createSession creates a new box session for chunk upload.
func (d *Destination) createSession(ctx context.Context, metaData metadata) error {
	sessionResponse, err := d.client.Session(ctx, metaData.filename, d.config.ParentID, metaData.fileID, metaData.filesize)
	if err != nil {
		return fmt.Errorf("error creating session: %w", err)
	}

	d.sessions[metaData.hash] = session{
		sessionID:  sessionResponse.ID,
		partSize:   sessionResponse.PartSize,
		totalParts: sessionResponse.TotalParts,
		parts:      []box.Part{},
		hasher:     sha1.New(), //nolint:gosec // box expects sha1 hash in api headers.
	}

	return nil
}

// uploadLargeParts uploads the chunked record to remote box directory. It caches the record content in memory
// and keeps on updating the cache by appending the new record bytes. Once the cache size equals the chunk part size
// it uploads the cached data and clears the cache for next records.
func (d *Destination) uploadParts(ctx context.Context, metaData metadata, content []byte) error {
	contentLength := len(content) + len(d.files[metaData.hash])
	switch {
	case metaData.index == 1:
		d.files[metaData.hash] = content

	case contentLength == d.sessions[metaData.hash].partSize:
		d.files[metaData.hash] = append(d.files[metaData.hash], content...)
		err := d.processPart(ctx, metaData)
		if err != nil {
			return err
		}

	case contentLength > d.sessions[metaData.hash].partSize:
		minus := d.sessions[metaData.hash].partSize - len(d.files[metaData.hash])
		d.files[metaData.hash] = append(d.files[metaData.hash], content[:minus]...)
		err := d.processPart(ctx, metaData)
		if err != nil {
			return err
		}
		d.files[metaData.hash] = append(d.files[metaData.hash], content[minus:]...)

	case contentLength < d.sessions[metaData.hash].partSize:
		d.files[metaData.hash] = append(d.files[metaData.hash], content...)
	}

	if metaData.index == metaData.totalChunks {
		if len(d.files[metaData.hash]) > 0 {
			err := d.processPart(ctx, metaData)
			if err != nil {
				return err
			}
		}

		digest := base64.StdEncoding.EncodeToString(d.sessions[metaData.hash].hasher.Sum(nil))
		_, err := d.client.CommitUpload(ctx, d.sessions[metaData.hash].sessionID, digest, d.sessions[metaData.hash].parts)
		if err != nil {
			return fmt.Errorf("error committing chunk upload: %w", err)
		}

		delete(d.sessions, metaData.hash)
		delete(d.files, metaData.hash)
	}

	return nil
}

// processPart process the chunk record by preparing the chunk upload request
// and updating the file session after a successful upload.
func (d *Destination) processPart(ctx context.Context, metaData metadata) error {
	shaSum := sha1.Sum(d.files[metaData.hash]) //nolint:gosec // box expects sha1 hash in api headers.
	shaB64 := base64.StdEncoding.EncodeToString(shaSum[:])

	start := d.sessions[metaData.hash].partsProcessed * d.sessions[metaData.hash].partSize
	var end int
	if metaData.index == metaData.totalChunks && len(d.files[metaData.hash]) > 0 {
		end = start + len(d.files[metaData.hash]) - 1
	} else {
		end = start + d.sessions[metaData.hash].partSize - 1
	}
	contentRange := fmt.Sprintf("bytes %d-%d/%d", start, end, metaData.filesize)

	resp, err := d.client.UploadChunk(ctx, d.files[metaData.hash], d.sessions[metaData.hash].sessionID, shaB64, contentRange)
	if err != nil {
		return fmt.Errorf("error uploading chunk: %w", err)
	}

	s := d.sessions[metaData.hash]
	s.partsProcessed++
	s.parts = append(s.parts, resp.Part)
	_, err = s.hasher.Write(d.files[metaData.hash])
	if err != nil {
		return fmt.Errorf("error hasher write: %w", err)
	}

	d.sessions[metaData.hash] = s
	d.files[metaData.hash] = []byte{}
	return nil
}

// cachedUpload is used to upload the files that are of 4MB > size < 20MB.
// It caches the chunked record in memory and uploads the file when the last chunk is appended.
func (d *Destination) cachedUpload(ctx context.Context, metaData metadata, content []byte) error {
	if metaData.index != 1 {
		_, ok := d.files[metaData.hash]
		if !ok {
			return NewInvalidChunkError("corrupt chunk")
		}
	}

	d.files[metaData.hash] = append(d.files[metaData.hash], content...)

	if metaData.index == metaData.totalChunks {
		response, err := d.client.Upload(ctx, metaData.filename, d.config.ParentID, metaData.fileID, d.files[metaData.hash])
		if err != nil {
			return fmt.Errorf("error uploading chunk: %w", err)
		}

		if response.Entries[0].Size != metaData.filesize {
			return NewInvalidFileError("invalid file upload")
		}

		// delete cached file after successful upload.
		delete(d.files, metaData.hash)
	}

	return nil
}

func (d *Destination) getFileIDByFilename(ctx context.Context, filename string) (string, error) {
	more := true
	var nextMarker string

	for more {
		var err error
		var items []box.Entry
		items, nextMarker, more, err = d.client.ListFolderItems(ctx, d.config.ParentID, nextMarker, 1000)
		if err != nil {
			return "", fmt.Errorf("error listing folder items %w", err)
		}
		for _, item := range items {
			fmt.Printf("got item name %s, our filename %s", item.Name, filename)
			if item.Name == filename {
				return item.ID, nil
			}
		}
	}

	return "", ErrFileNotFound
}

type metadata struct {
	filename    string
	filesize    int64
	index       int
	totalChunks int
	hash        string
	fileID      string
}

// extractMetadata extracts the metadata fields from the record.
func (d *Destination) extractMetadata(record opencdc.Record) (metadata, error) {
	meta := metadata{}
	var ok bool
	chunked, ok := record.Metadata[opencdc.MetadataFileChunked]
	if ok && chunked == "true" {
		chunkIndex, ok := record.Metadata[opencdc.MetadataFileChunkIndex]
		if !ok {
			return metadata{}, NewInvalidChunkError("chunk index not found")
		}
		var err error
		meta.index, err = strconv.Atoi(chunkIndex)
		if err != nil {
			return metadata{}, fmt.Errorf("failed to parse chunk index: %w", err)
		}
		total, ok := record.Metadata[opencdc.MetadataFileChunkCount]
		if !ok {
			return metadata{}, NewInvalidChunkError("total_chunk not found")
		}
		meta.totalChunks, err = strconv.Atoi(total)
		if err != nil {
			return metadata{}, fmt.Errorf("failed to parse total chunks: %w", err)
		}
	}

	meta.hash, ok = record.Metadata[opencdc.MetadataFileHash]
	if !ok {
		return metadata{}, NewInvalidChunkError("hash not found")
	}
	meta.filename, ok = record.Metadata[opencdc.MetadataFileName]
	if !ok {
		return metadata{}, NewInvalidChunkError("filename not found")
	}
	fileSize, ok := record.Metadata[opencdc.MetadataFileSize]
	if !ok {
		return metadata{}, NewInvalidChunkError("file size not found")
	}
	var err error
	meta.filesize, err = strconv.ParseInt(fileSize, 10, 64)
	if err != nil {
		return metadata{}, fmt.Errorf("failed to parse file size: %w", err)
	}

	return meta, nil
}
