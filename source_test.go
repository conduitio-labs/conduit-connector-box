package box_test

import (
	"context"
	"testing"

	box "github.com/conduitio-labs/conduit-connector-box"
	"github.com/matryer/is"
)

func TestTeardownSource_NoOpen(t *testing.T) {
	is := is.New(t)
	con := box.NewSource()
	err := con.Teardown(context.Background())
	is.NoErr(err)
}
