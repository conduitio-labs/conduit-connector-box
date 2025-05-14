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
	"encoding/json"
	"fmt"

	"github.com/conduitio/conduit-commons/opencdc"
)

type ChunkInfo struct {
	FileID      string `json:"file_id"`
	Hash        string `json:"hash"`
	ChunkIndex  int    `json:"chunk_index"`
	TotalChunks int    `json:"total_chunks"`
}

type Position struct {
	StreamPosition        int        `json:"stream_position"`
	ChunkInfo             *ChunkInfo `json:"chunk_info"`
	LastProcessedUnixTime int64      `json:"last_processed_unix_time"`
}

func NewPosition() *Position {
	return &Position{}
}

func ParseSDKPosition(position opencdc.Position) (*Position, error) {
	var pos Position
	if position == nil {
		return NewPosition(), nil
	}

	if err := json.Unmarshal(position, &pos); err != nil {
		return nil, fmt.Errorf("unmarshal opencdc.Position into Position: %w", err)
	}
	return &pos, nil
}

func ToSDKPosition(streamPosition int, chunkInfo *ChunkInfo, lastProcessed int64) (opencdc.Position, error) {
	p := &Position{
		StreamPosition:        streamPosition,
		ChunkInfo:             chunkInfo,
		LastProcessedUnixTime: lastProcessed,
	}

	positionBytes, err := json.Marshal(p)
	if err != nil {
		return nil, fmt.Errorf("marshal position: %w", err)
	}
	return positionBytes, nil
}
