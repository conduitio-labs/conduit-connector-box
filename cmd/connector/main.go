package main

import (
	box "github.com/conduitio-labs/conduit-connector-box"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

func main() {
	sdk.Serve(box.Connector)
}
