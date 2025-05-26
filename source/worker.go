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
	"sync"
	"time"

	"github.com/conduitio-labs/conduit-connector-box/pkg/box"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

const (
	typeFolder  = "folder"
	typeFile    = "file"
	typeDeleted = "deleted"
	itemCreate  = "ITEM_CREATE"
	itemUpload  = "ITEM_UPLOAD"
	itemModify  = "ITEM_MODIFY"
	itemRename  = "ITEM_RENAME"
	itemTrash   = "ITEM_TRASH"
)

type Worker struct {
	client    box.Box
	config    Config
	recordsCh chan<- opencdc.Record
	wg        *sync.WaitGroup

	streamPosition    int
	currentChunkInfo  *ChunkInfo
	lastProcessedTime int64
}

func NewWorker(
	client box.Box,
	config Config,
	position *Position,
	recordsCh chan<- opencdc.Record,
	wg *sync.WaitGroup,
) *Worker {
	return &Worker{
		client:            client,
		config:            config,
		recordsCh:         recordsCh,
		wg:                wg,
		streamPosition:    position.StreamPosition,
		currentChunkInfo:  position.ChunkInfo,
		lastProcessedTime: position.LastProcessedUnixTime,
	}
}

func (w *Worker) Start(ctx context.Context) {
	defer w.wg.Done()
	retries := w.config.Retries

	for {
		waitDuration := w.config.PollingInterval

		err := w.process(ctx)
		if err != nil {
			if retries == 0 {
				sdk.Logger(ctx).Err(err).Msg("retries exhausted, worker shutting down...")
				return
			}
			retries--
			sdk.Logger(ctx).Warn().Err(err).Msgf("retrying... (%d attempts left)", retries)
			waitDuration = w.config.RetryDelay
		} else {
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

func (w *Worker) process(ctx context.Context) error {
	if w.streamPosition == 0 {
		return w.snapshot(ctx)
	}

	return w.cdc(ctx)
}

func (w *Worker) snapshot(ctx context.Context) error {
	marker := ""
	for {
		items, nextMarker, hasMore, err := w.client.ListFolderItems(ctx, w.config.ParentID, marker, *w.config.BatchSize)
		if err != nil {
			return fmt.Errorf("list folder items failed: %w", err)
		}

		// Get current stream position.
		_, currentPosition, err := w.client.GetEvents(ctx, w.streamPosition)
		if err != nil {
			return fmt.Errorf("get latest events failed: %w", err)
		}
		w.streamPosition = currentPosition

		for _, item := range items {
			if err := w.processFile(ctx, item); err != nil {
				return fmt.Errorf("process file failed: %w", err)
			}
		}

		if !hasMore {
			break
		}
		marker = nextMarker
	}

	return nil
}

func (w *Worker) cdc(ctx context.Context) error {
	events, nextPosition, err := w.client.GetEvents(ctx, w.streamPosition)
	if err != nil {
		return fmt.Errorf("get latest events failed: %w", err)
	}
	w.streamPosition = nextPosition

	for _, event := range events {
		if w.shouldSkipEvent(event) {
			continue
		}

		switch event.Type {
		case itemCreate, itemUpload, itemModify, itemRename:
			if err := w.processFile(ctx, event.Source); err != nil {
				return fmt.Errorf("process file failed: %w", err)
			}
		case itemTrash:
			if err := w.processDeletedFile(ctx, event.Source); err != nil {
				return fmt.Errorf("process deleted file failed: %w", err)
			}
		}
	}

	return nil
}

func (w *Worker) processFile(ctx context.Context, entry box.Entry) error {
	if entry.Size > w.config.FileChunkSizeBytes {
		return w.processChunkedFile(ctx, entry)
	}
	return w.processFullFile(ctx, entry)
}

func (w *Worker) processChunkedFile(ctx context.Context, entry box.Entry) error {
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

		record, err := w.createChunkedRecord(entry, chunkIdx, totalChunks, chunkData)
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

func (w *Worker) processFullFile(ctx context.Context, entry box.Entry) error {
	fmt.Println("processing file ----- ", entry.Name)
	fileData, err := w.downloadChunk(ctx, entry.ID, "")
	if err != nil {
		return fmt.Errorf("download failed: %w", err)
	}

	record, err := w.createRecord(entry, fileData)
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

func (w *Worker) processDeletedFile(ctx context.Context, entry box.Entry) error {
	w.currentChunkInfo = nil
	position, err := ToSDKPosition(w.streamPosition, nil, w.lastProcessedTime)
	if err != nil {
		return fmt.Errorf("marshal position: %w", err)
	}

	metadata := opencdc.Metadata{
		opencdc.MetadataFileName:   entry.Name,
		"file_id":                  entry.ID,
		opencdc.MetadataFileHash:   entry.Sha1,
		opencdc.MetadataCollection: entry.Parent.ID,
	}

	record := sdk.Util.Source.NewRecordDelete(
		position,
		metadata,
		opencdc.StructuredData{"id": entry.ID, "hash": entry.Sha1},
		nil,
	)

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

func (w *Worker) createChunkedRecord(entry box.Entry, chunkIdx, totalChunks int, data []byte) (opencdc.Record, error) {
	var chunkInfo *ChunkInfo

	if chunkIdx == totalChunks {
		w.currentChunkInfo = nil
		w.lastProcessedTime = entry.ModifiedAt.UnixNano()
	} else {
		chunkInfo = &ChunkInfo{
			FileID:      entry.ID,
			Hash:        entry.Sha1,
			ChunkIndex:  chunkIdx,
			TotalChunks: totalChunks,
		}
		w.currentChunkInfo = chunkInfo
	}

	sdkPosition, err := ToSDKPosition(w.streamPosition, chunkInfo, w.lastProcessedTime)
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

	return sdk.Util.Source.NewRecordCreate(
		sdkPosition,
		metadata,
		opencdc.StructuredData{"id": entry.ID, "hash": entry.Sha1},
		opencdc.RawData(data),
	), nil
}

func (w *Worker) createRecord(entry box.Entry, data []byte) (opencdc.Record, error) {
	w.currentChunkInfo = nil
	w.lastProcessedTime = entry.ModifiedAt.UnixNano()

	position, err := ToSDKPosition(w.streamPosition, nil, w.lastProcessedTime)
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

	return sdk.Util.Source.NewRecordCreate(
		position,
		metadata,
		opencdc.StructuredData{"id": entry.ID, "hash": entry.Sha1},
		opencdc.RawData(data),
	), nil
}

func (w *Worker) shouldSkipEvent(event box.Event) bool {
	if event.Source.Type != typeFile ||
		event.CreatedAt.UnixNano() < w.lastProcessedTime ||
		event.Source.Parent.ID != fmt.Sprintf("%d", w.config.ParentID) {
		return true
	}

	return false
}
