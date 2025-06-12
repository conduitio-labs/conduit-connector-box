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

package source

import (
	"context"
	"fmt"
	"io"
	"strconv"
	"sync"
	"time"

	"github.com/conduitio-labs/conduit-connector-box/pkg/box"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

const (
	typeFolder = "folder"
	typeFile   = "file"
)

type Worker struct {
	client           box.Box
	config           Config
	recordsCh        chan<- opencdc.Record
	errorCh          chan<- error
	wg               *sync.WaitGroup
	currentChunkInfo *ChunkInfo
	lastModifiedAt   time.Time
}

func NewWorker(
	client box.Box,
	config Config,
	position *Position,
	recordsCh chan<- opencdc.Record,
	errorCh chan<- error,
	wg *sync.WaitGroup,
) *Worker {
	return &Worker{
		client:           client,
		config:           config,
		recordsCh:        recordsCh,
		errorCh:          errorCh,
		wg:               wg,
		currentChunkInfo: position.ChunkInfo,
		lastModifiedAt:   position.LastModifiedAt,
	}
}

func (w *Worker) Start(ctx context.Context) {
	defer func() {
		close(w.recordsCh)
		w.wg.Done()
	}()
	retries := w.config.Retries

	for {
		modifiedFrom := w.lastModifiedAt
		if modifiedFrom.IsZero() {
			modifiedFrom = time.Time{}
		}
		modifiedTo := time.Now()

		waitDuration := w.config.PollingInterval

		err := w.processFolder(ctx, w.config.ParentID, modifiedFrom, modifiedTo)
		if err != nil {
			if retries == 0 {
				sdk.Logger(ctx).Err(err).Msg("retries exhausted, worker shutting down...")
				w.errorCh <- err
				return
			}
			retries--
			sdk.Logger(ctx).Warn().Err(err).Msgf("retrying... (%d attempts left)", retries)
			waitDuration = w.config.RetryDelay
		} else {
			w.lastModifiedAt = modifiedTo
			retries = w.config.Retries // Reset retries on success
		}

		select {
		case <-ctx.Done():
			sdk.Logger(ctx).Debug().Msg("context canceled, worker shutting down...")
			return
		case <-time.After(waitDuration):
		}
	}
}

func (w *Worker) processFolder(ctx context.Context, folderID int, modifiedFrom, modifiedTo time.Time) error {
	sdk.Logger(ctx).Debug().
		Int("folder_id", folderID).
		Msgf("processing folder")

	marker := ""
	for {
		response, err := w.client.ListFolderItems(ctx, folderID, marker, *w.config.BatchSize)
		if err != nil {
			return fmt.Errorf("list folder items failed: %w", err)
		}

		for _, item := range response.Entries {
			if err := w.processItem(ctx, item, modifiedFrom, modifiedTo); err != nil {
				return fmt.Errorf("process file failed: %w", err)
			}
		}

		if response.NextMarker == "" {
			break
		}
		marker = response.NextMarker
	}

	return nil
}

func (w *Worker) processItem(ctx context.Context, entry box.Entry, modifiedFrom, modifiedTo time.Time) error {
	switch entry.Type {
	case typeFolder:
		folderID, err := strconv.Atoi(entry.ID)
		if err != nil {
			return fmt.Errorf("invalid folder id: %w", err)
		}
		return w.processFolder(ctx, folderID, modifiedFrom, modifiedTo)
	case typeFile:
		return w.processFile(ctx, entry, modifiedFrom, modifiedTo)
	default:
		sdk.Logger(ctx).Trace().Msgf("ignoring item type: %v", entry.Type)
		return nil
	}
}

func (w *Worker) processFile(ctx context.Context, entry box.Entry, modFrom, modifiedTo time.Time) error {
	// If the entry was modified before modFrom, the entry was already read.
	// If the entry was modified after modifiedTo, we skip it because we are
	// limited to the range [modFrom, modifiedTo).
	if entry.ModifiedAt.Before(modFrom) || entry.ModifiedAt.After(modifiedTo) || entry.ModifiedAt.Equal(modifiedTo) {
		return nil
	}

	// If the entry was created in this time range, we record that as a create operation.
	// Otherwise, it's an update operation.
	isAnUpdate := entry.CreatedAt.Before(modFrom)

	err := w.processFileAnySize(ctx, entry, isAnUpdate)
	if err != nil {
		return fmt.Errorf("process file failed: %w", err)
	}

	return nil
}

func (w *Worker) processFileAnySize(ctx context.Context, entry box.Entry, existing bool) error {
	sdk.Logger(ctx).Trace().
		Str("file_id", entry.ID).
		Msg("processing file")

	if entry.Size > w.config.FileChunkSizeBytes {
		return w.processChunkedFile(ctx, entry, existing)
	}

	return w.processFullFile(ctx, entry, existing)
}

func (w *Worker) processChunkedFile(ctx context.Context, entry box.Entry, existing bool) error {
	totalChunks := (entry.Size + w.config.FileChunkSizeBytes - 1) / w.config.FileChunkSizeBytes

	startChunk := 1
	if w.currentChunkInfo != nil && w.currentChunkInfo.FileID == entry.ID {
		startChunk = w.currentChunkInfo.ChunkIndex + 1
	}

	for chunkIdx := startChunk; chunkIdx <= totalChunks; chunkIdx++ {
		start := (chunkIdx - 1) * w.config.FileChunkSizeBytes
		end := min(start+w.config.FileChunkSizeBytes, entry.Size)

		rangeHeader := fmt.Sprintf("bytes=%d-%d", start, end-1)
		chunkData, err := w.downloadChunk(ctx, entry.ID, rangeHeader)
		if err != nil {
			return fmt.Errorf("download chunk %d failed: %w", chunkIdx, err)
		}

		record, err := w.createChunkedRecord(entry, chunkIdx, totalChunks, chunkData, existing)
		if err != nil {
			return fmt.Errorf("create record failed: %w", err)
		}

		select {
		case w.recordsCh <- record:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func (w *Worker) processFullFile(ctx context.Context, entry box.Entry, existing bool) error {
	fileData, err := w.downloadChunk(ctx, entry.ID, "")
	if err != nil {
		return fmt.Errorf("download failed: %w", err)
	}

	record, err := w.createRecord(entry, fileData, existing)
	if err != nil {
		return fmt.Errorf("create record failed: %w", err)
	}

	select {
	case w.recordsCh <- record:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (w *Worker) downloadChunk(ctx context.Context, fileID string, rangeHeader string) ([]byte, error) {
	reader, err := w.client.Download(ctx, fileID, rangeHeader)
	if err != nil {
		return nil, fmt.Errorf("download file %q: %w", fileID, err)
	}
	defer reader.Close()

	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("read file %q: %w", fileID, err)
	}

	return data, nil
}

func (w *Worker) createChunkedRecord(entry box.Entry, chunkIdx, totalChunks int, data []byte, existing bool) (opencdc.Record, error) {
	var chunkInfo *ChunkInfo

	if chunkIdx == totalChunks {
		w.currentChunkInfo = nil
	} else {
		chunkInfo = &ChunkInfo{
			FileID:      entry.ID,
			Hash:        entry.Sha1,
			ChunkIndex:  chunkIdx,
			TotalChunks: totalChunks,
		}
		w.currentChunkInfo = chunkInfo
	}

	sdkPosition, err := ToSDKPosition(w.lastModifiedAt, nil)
	if err != nil {
		return opencdc.Record{}, fmt.Errorf("marshal position: %w", err)
	}

	metadata := opencdc.Metadata{
		opencdc.MetadataFileName:       entry.Name,
		"parent":                       entry.Parent.Name,
		"file_id":                      entry.ID,
		opencdc.MetadataCollection:     entry.Parent.ID,
		opencdc.MetadataFileSize:       fmt.Sprintf("%d", entry.Size),
		opencdc.MetadataFileHash:       entry.Sha1,
		opencdc.MetadataFileChunkIndex: fmt.Sprintf("%d", chunkIdx),
		opencdc.MetadataFileChunkCount: fmt.Sprintf("%d", totalChunks),
		opencdc.MetadataFileChunked:    "true",
	}

	record := sdk.Util.Source.NewRecordCreate(
		sdkPosition,
		metadata,
		opencdc.StructuredData{"id": entry.ID, "hash": entry.Sha1},
		opencdc.RawData(data),
	)

	if existing {
		record = sdk.Util.Source.NewRecordUpdate(
			sdkPosition,
			metadata,
			opencdc.StructuredData{"id": entry.ID, "hash": entry.Sha1},
			nil,
			opencdc.RawData(data),
		)
	}

	return record, nil
}

func (w *Worker) createRecord(entry box.Entry, data []byte, existing bool) (opencdc.Record, error) {
	w.currentChunkInfo = nil

	position, err := ToSDKPosition(w.lastModifiedAt, nil)
	if err != nil {
		return opencdc.Record{}, fmt.Errorf("marshal position: %w", err)
	}

	metadata := opencdc.Metadata{
		opencdc.MetadataFileName:   entry.Name,
		"parent":                   entry.Parent.Name,
		"file_id":                  entry.ID,
		opencdc.MetadataCollection: entry.Parent.ID,
		opencdc.MetadataFileSize:   fmt.Sprintf("%d", entry.Size),
		opencdc.MetadataFileHash:   entry.Sha1,
	}

	record := sdk.Util.Source.NewRecordCreate(
		position,
		metadata,
		opencdc.StructuredData{"id": entry.ID, "hash": entry.Sha1},
		opencdc.RawData(data),
	)

	if existing {
		record = sdk.Util.Source.NewRecordUpdate(
			position,
			metadata,
			opencdc.StructuredData{"id": entry.ID, "hash": entry.Sha1},
			nil,
			opencdc.RawData(data),
		)
	}

	return record, nil
}
