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
	"crypto/sha1"
	"encoding/base64"
	"fmt"
	"strconv"

	"github.com/conduitio-labs/conduit-connector-box/config"
	"github.com/conduitio-labs/conduit-connector-box/pkg/box"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

var (
	maxRecordSize      = 4 * 1024 * 1024  // 4MB
	minChunkUploadSize = 20 * 1024 * 1024 // 20MB
)

type Destination struct {
	sdk.UnimplementedDestination

	config   DestinationConfig
	client   box.Box
	sessions map[string]session
	// files caches a file in memory. For Files > 4MB and < 20MB we append
	// the chunked records and then upload it using box upload endpoint.
	files map[string][]byte
}

type session struct {
	sessionID  string
	partSize   int64
	totalParts int64
	parts      []box.Part
}

type DestinationConfig struct {
	sdk.DefaultDestinationMiddleware
	// Config includes parameters that are the same in the source and destination.
	config.Config
}

func NewDestination() sdk.Destination {
	// Create Destination and wrap it in the default middleware.
	return sdk.DestinationWithMiddleware(&Destination{})
}

func (d *Destination) Config() sdk.DestinationConfig {
	return &d.config
}

func (d *Destination) Open(ctx context.Context) error {
	sdk.Logger(ctx).Info().Msg("Opening Box destination")
	var err error
	if d.client == nil {
		d.client, err = box.NewHTTPClient(d.config.Token)
		if err != nil {
			return fmt.Errorf("error creating box client: %w", err)
		}
	}
	d.sessions = make(map[string]session)
	d.files = make(map[string][]byte)
	return nil
}

func (d *Destination) Write(ctx context.Context, records []opencdc.Record) (int, error) {
	for i, record := range records {
		switch {
		case record.Metadata["is_chunked"] == "true":
			err := d.handleFileChunk(ctx, record)
			if err != nil {
				return i, fmt.Errorf("failed to upload file chunk: %w", err)
			}

		default:
			err := d.uploadFile(ctx, record)
			if err != nil {
				return i, fmt.Errorf("failed to upload file: %w", err)
			}
		}
	}
	return len(records), nil
}

func (d *Destination) Teardown(ctx context.Context) error {
	sdk.Logger(ctx).Info().Msg("Tearing down Box destination")
	return nil
}

func (d *Destination) uploadFile(ctx context.Context, r opencdc.Record) error {
	filename, ok := r.Metadata["filename"]
	if !ok {
		return NewInvalidFileError("missing filename")
	}
	size, ok := r.Metadata["file_size"]
	if !ok {
		return NewInvalidFileError("missing file_size")
	}
	filesize, err := strconv.ParseInt(size, 10, 64)
	if err != nil {
		return NewInvalidFileError("invalid file_size" + err.Error())
	}

	response, err := d.client.Upload(ctx, filename, d.config.ParentID, r.Payload.After.Bytes())
	if err != nil {
		return fmt.Errorf("error uploading file: %w", err)
	}

	if response.Entries[0].Size != filesize {
		return NewInvalidFileError("filesize mismatch")
	}

	return nil
}

func (d *Destination) handleFileChunk(ctx context.Context, r opencdc.Record) error {
	metaData, err := d.extractMetadata(r)
	if err != nil {
		return err
	}

	if metaData.filesize < int64(minChunkUploadSize) {
		return d.cachedUpload(ctx, r)
	}

	if metaData.filesize >= int64(minChunkUploadSize) {
		sess, ok := d.sessions[metaData.hash]
		if !ok && metaData.index == 1 {
			sessionResponse, err := d.client.Session(ctx, metaData.filename, d.config.ParentID, metaData.filesize)
			if err != nil {
				return err
			}
			d.sessions[metaData.hash] = session{
				sessionID:  sessionResponse.ID,
				partSize:   sessionResponse.PartSize,
				totalParts: sessionResponse.TotalParts,
				parts:      []box.Part{},
			}
			sess = d.sessions[metaData.hash]
		}

		if sess.partSize <= int64(maxRecordSize) {
			// upload chunk one by one
			shaSum := sha1.Sum(r.Payload.After.Bytes())
			shaB64 := base64.StdEncoding.EncodeToString(shaSum[:])
			start := metaData.index * sess.partSize
			end := start + sess.partSize - 1
			contentRange := fmt.Sprintf("bytes %d-%d/%d", start, end, metaData.filesize)

			_, err := d.client.UploadChunk(ctx, r.Payload.After.Bytes(), sess.sessionID, shaB64, contentRange)
			if err != nil {
				return err
			}

			if metaData.index == metaData.totalChunks {
				_, err := d.client.CommitUpload(ctx, sess.sessionID, sess.parts)
				if err != nil {
					return err
				}
				// delete session
				delete(d.sessions, metaData.hash)
			}

			return nil
		}

		return d.uploadLargeParts(ctx, sess, metaData, r.Payload.After.Bytes())
	}

	return nil
}

func (d *Destination) uploadLargeParts(ctx context.Context, sess session, metaData metadata, content []byte) error {
	if metaData.index == 1 {
		d.files[metaData.hash] = content
		return nil
	}

	switch {
	case len(content)+len(d.files[metaData.hash]) == int(sess.partSize):
		d.files[metaData.hash] = append(d.files[metaData.hash], content...)

		shaSum := sha1.Sum(content)
		shaB64 := base64.StdEncoding.EncodeToString(shaSum[:])
		start := metaData.index * sess.partSize
		end := start + sess.partSize - 1
		contentRange := fmt.Sprintf("bytes %d-%d/%d", start, end, metaData.filesize)

		_, err := d.client.UploadChunk(ctx, d.files[metaData.hash], sess.sessionID, shaB64, contentRange)
		if err != nil {
			return err
		}

		d.files[metaData.hash] = []byte{}

	case len(content)+len(d.files[metaData.hash]) > int(sess.partSize):
		minus := int(sess.partSize) - len(content)
		d.files[metaData.hash] = append(d.files[metaData.hash], content[:minus]...)

		shaSum := sha1.Sum(content)
		shaB64 := base64.StdEncoding.EncodeToString(shaSum[:])
		start := metaData.index * sess.partSize
		end := start + sess.partSize - 1
		contentRange := fmt.Sprintf("bytes %d-%d/%d", start, end, metaData.filesize)

		_, err := d.client.UploadChunk(ctx, d.files[metaData.hash], sess.sessionID, shaB64, contentRange)
		if err != nil {
			return err
		}

		d.files[metaData.hash] = []byte{}
		d.files[metaData.hash] = append(d.files[metaData.hash], content[minus:]...)

	case len(content)+len(d.files[metaData.hash]) < int(sess.partSize):
		d.files[metaData.hash] = append(d.files[metaData.hash], content...)
	}

	return nil
}

// cachedUpload is used to upload the files that are of 4MB > size < 20MB.
// It caches the chunked record in memory and uploads the file when the last chunk is appended.
func (d *Destination) cachedUpload(ctx context.Context, r opencdc.Record) error {
	metaData, err := d.extractMetadata(r)
	if err != nil {
		return err
	}

	if metaData.index != 1 {
		_, ok := d.files[metaData.hash]
		if !ok {
			return NewInvalidChunkError("corrupt chunk")
		}
	}

	d.files[metaData.hash] = append(d.files[metaData.hash], r.Payload.After.Bytes()...)

	if metaData.index == metaData.totalChunks {
		response, err := d.client.Upload(ctx, metaData.filename, d.config.ParentID, d.files[metaData.hash])
		if err != nil {
			return err
		}

		if response.Entries[0].Size != metaData.filesize {
			return NewInvalidFileError("invalid file upload")
		}

		// delete cached file after successful upload.
		delete(d.files, metaData.hash)
	}

	return nil
}

// func (d *Destination) uploadInitialChunk(ctx context.Context, metaData metadata, content []byte) error {
// 	// create a session
// 	sessionResponse, err := d.client.Session(ctx, metaData.filename, d.config.ParentID, metaData.filesize)
// 	if err != nil {
// 		return fmt.Errorf("error creating upload session: %w", err)
// 	}

// 	// write first chunk
// 	shaSum := sha1.Sum(content)
// 	shaB64 := base64.StdEncoding.EncodeToString(shaSum[:])
// 	contentRange := fmt.Sprintf("bytes %d-%d/%d", 0, int64(sessionResponse.PartSize)-1, metaData.filesize)
// 	uploadResponse, err := d.client.UploadChunk(ctx, content, sessionResponse.ID, shaB64, contentRange)
// 	if err != nil {
// 		return err
// 	}

// 	// cache the session
// 	d.sessions[metaData.hash] = session{
// 		sessionID: sessionResponse.ID,
// 		part: box.Part{
// 			PartID: uploadResponse.Part.PartID,
// 			Offset: uploadResponse.Part.Offset,
// 			Size:   uploadResponse.Part.Size,
// 			Sha1:   uploadResponse.Part.Sha1,
// 		},
// 	}

// 	return nil
// }

type metadata struct {
	filename    string
	filesize    int64
	index       int64
	totalChunks int64
	hash        string
}

func (d *Destination) extractMetadata(record opencdc.Record) (metadata, error) {
	meta := metadata{}
	var ok bool
	chunked, ok := record.Metadata["is_chunked"]
	if ok && chunked == "true" {
		chunkIndex, ok := record.Metadata["chunk_index"]
		if !ok {
			return metadata{}, NewInvalidChunkError("chunk_index not found")
		}
		var err error
		meta.index, err = strconv.ParseInt(chunkIndex, 10, 64)
		if err != nil {
			return metadata{}, fmt.Errorf("failed to parse chunk_index: %w", err)
		}
		total, ok := record.Metadata["total_chunks"]
		if !ok {
			return metadata{}, NewInvalidChunkError("total_chunk not found")
		}
		meta.totalChunks, err = strconv.ParseInt(total, 10, 64)
		if err != nil {
			return metadata{}, fmt.Errorf("failed to parse total_chunks: %w", err)
		}
	}

	meta.hash, ok = record.Metadata["hash"]
	if !ok {
		return metadata{}, NewInvalidChunkError("hash not found")
	}
	meta.filename, ok = record.Metadata["filename"]
	if !ok {
		return metadata{}, NewInvalidChunkError("hash not found")
	}
	fileSize, ok := record.Metadata["file_size"]
	if !ok {
		return metadata{}, NewInvalidChunkError("file_size not found")
	}
	var err error
	meta.filesize, err = strconv.ParseInt(fileSize, 10, 64)
	if err != nil {
		return metadata{}, fmt.Errorf("failed to parse file_size: %w", err)
	}

	return meta, nil
}
