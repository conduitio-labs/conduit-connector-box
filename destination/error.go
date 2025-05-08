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
	"fmt"
)

var (
	ErrMissingFilename = fmt.Errorf("missing metadata filename")
	ErrInvalidHash     = fmt.Errorf("missing or invalid content hash")
	ErrInvalidSession  = fmt.Errorf("invalid session")
)

type InvalidChunkError struct {
	message string
}

func (e InvalidChunkError) Error() string {
	return fmt.Sprintf("invalid chunk: %s", e.message)
}

func NewInvalidChunkError(msg string) InvalidChunkError {
	return InvalidChunkError{msg}
}

type InvalidFileError struct {
	message string
}

func (e InvalidFileError) Error() string {
	return fmt.Sprintf("invalid file record: %s", e.message)
}

func NewInvalidFileError(msg string) InvalidFileError {
	return InvalidFileError{msg}
}
