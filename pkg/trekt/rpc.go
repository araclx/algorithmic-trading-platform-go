// Copyright 2019 REKTRA Network, All Rights Reserved.

package trekt

// RPCClient represents RPC-client to request call and receive result.
type RPCClient interface {
	// Close closes the client.
	Close()

	// It makes call request and notifies about the result by calling provided
	// callbacks.
	Request(
		routingKey string,
		mandatory bool,
		request interface{},
		handleSuccess func([]byte),
		handleFail func(error))
}
