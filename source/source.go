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
	"sync"
	"time"

	"github.com/conduitio-labs/conduit-connector-box/config"
	"github.com/conduitio-labs/conduit-connector-box/pkg/box"
	"github.com/conduitio/conduit-commons/lang"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

var (
	ErrSourceClosed     = fmt.Errorf("error source not opened for reading")
	ErrReadingData      = fmt.Errorf("error reading data")
	ErrInvalidBatchSize = fmt.Errorf("batch size is out of allowed range [1-1000]")
)

type Source struct {
	sdk.UnimplementedSource

	config    Config
	position  *Position
	client    box.Box
	ch        chan opencdc.Record
	errCh     chan error
	workersWg *sync.WaitGroup
}

type Config struct {
	sdk.DefaultSourceMiddleware
	config.Config

	// This period is used by worker to poll for new data at regular intervals.
	PollingInterval time.Duration `json:"pollingInterval" default:"5s"`
	// Size of a file chunk in bytes to split large files, maximum is 4MB.
	FileChunkSizeBytes int `json:"fileChunkSizeBytes" default:"3932160"`
	// Maximum number of retry attempts.
	Retries int `json:"retries" default:"0"`
	// Delay between retry attempts.
	RetryDelay time.Duration `json:"retryDelay" default:"10s"`
}

// Validate checks if the configuration values are within allowed Box limits.
func (c *Config) Validate(_ context.Context) error {
	// c.BatchSize must be 1-1000 per Box API requirements.
	// Docs: https://developer.box.com/reference/get-folders-id-items/
	if c.BatchSize == nil || *c.BatchSize < 1 || *c.BatchSize > 1000 {
		return ErrInvalidBatchSize
	}

	return nil
}

func NewSource() sdk.Source {
	return sdk.SourceWithMiddleware(&Source{
		config: Config{
			DefaultSourceMiddleware: sdk.DefaultSourceMiddleware{
				// disable schema extraction by default, as the source produces raw payload data
				SourceWithSchemaExtraction: sdk.SourceWithSchemaExtraction{
					PayloadEnabled: lang.Ptr(false),
				},
			},
		},
	})
}

func (s *Source) Config() sdk.SourceConfig {
	return &s.config
}

func (s *Source) Open(ctx context.Context, position opencdc.Position) error {
	sdk.Logger(ctx).Info().Msg("Opening Box source")

	var err error
	s.position, err = ParseSDKPosition(position)
	if err != nil {
		return fmt.Errorf("error parsing sdk position: %w", err)
	}

	if s.client == nil {
		s.client, err = box.NewHTTPClient(s.config.Token)
		if err != nil {
			return fmt.Errorf("error creating http client for box: %w", err)
		}
	}

	// Verify folder exists
	if s.config.ParentID != 0 {
		isFolder, err := s.client.VerifyFolder(ctx, s.config.ParentID)
		if err != nil || !isFolder {
			return fmt.Errorf("error verifying folder id: %w", err)
		}
	}

	s.ch = make(chan opencdc.Record, 5)
	s.errCh = make(chan error)
	s.workersWg = &sync.WaitGroup{}

	// Start worker
	s.workersWg.Add(1)
	go func() {
		NewWorker(
			s.client,
			s.config,
			s.position,
			s.ch,
			s.errCh,
			s.workersWg,
		).Start(ctx)
	}()

	return nil
}

func (s *Source) ReadN(ctx context.Context, n int) ([]opencdc.Record, error) {
	if s.ch == nil {
		return nil, ErrSourceClosed
	}

	records := make([]opencdc.Record, 0, n)
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case err := <-s.errCh:
		return nil, fmt.Errorf("worker error: %w", err)
	case r, ok := <-s.ch:
		if !ok {
			return nil, ErrReadingData
		}
		records = append(records, r)
	}

	for len(records) < n {
		select {
		case err := <-s.errCh:
			return records, fmt.Errorf("worker error: %w", err)
		case r, ok := <-s.ch:
			if !ok {
				break
			}
			records = append(records, r)
		default:
			return records, nil
		}
	}

	return records, nil
}

func (s *Source) Ack(ctx context.Context, position opencdc.Position) error {
	sdk.Logger(ctx).Trace().Str("position", string(position)).Msg("got ack")
	return nil
}

func (s *Source) Teardown(ctx context.Context) error {
	sdk.Logger(ctx).Info().Msg("Tearing down Box source")
	if s.client != nil {
		s.client.Close()
	}
	if s.workersWg != nil {
		s.workersWg.Wait()
	}
	if s.ch != nil {
		close(s.ch)
		s.ch = nil
	}
	if s.errCh != nil {
		close(s.errCh)
		s.errCh = nil
	}
	return nil
}
