package box_test

import (
	"context"
	"testing"

	box "github.com/conduitio-labs/conduit-connector-box"
	"github.com/matryer/is"
)

func TestTeardown_NoOpen(t *testing.T) {
	is := is.New(t)
	con := box.NewDestination()
	err := con.Teardown(context.Background())
	is.NoErr(err)
}
