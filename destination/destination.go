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
	"fmt"
	"hash"

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
	// It is also used to cache chunks for chunked upload.
	files map[string][]byte
}

type session struct {
	sessionID      string
	partSize       int
	totalParts     int
	parts          []box.Part
	partsProcessed int
	hasher         hash.Hash
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
		case record.Operation == opencdc.OperationDelete:
			err := d.client.Delete(ctx, record.Metadata["box.file_id"])
			if err != nil {
				return i, fmt.Errorf("failed to delete file: %w", err)
			}

		default:
			if record.Metadata["is_chunked"] == "true" {
				err := d.handleFileChunk(ctx, record)
				if err != nil {
					return i, fmt.Errorf("failed to upload file chunk: %w", err)
				}
			} else {
				err := d.uploadFile(ctx, record)
				if err != nil {
					return i, fmt.Errorf("failed to upload file: %w", err)
				}
			}
		}
	}
	return len(records), nil
}

func (d *Destination) Teardown(ctx context.Context) error {
	sdk.Logger(ctx).Info().Msg("Tearing down Box destination")
	return nil
}
