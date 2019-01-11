// Copyright 2018 REKTRA Network, All Rights Reserved.

package mqclient

import (
	"encoding/json"
	"errors"
	"os"

	"github.com/streadway/amqp"
)

///////////////////////////////////////////////////////////////////////////////

// Auth provides authorization information.
type Auth struct {
	Login               string
	IsMarketDataAllowed bool
}

// AuthRequest provides authorization request parameters.
type AuthRequest struct {
	Login    string
	Password string
}

///////////////////////////////////////////////////////////////////////////////

// AuthExchange represents authorization exchange.
type AuthExchange struct {
	exchange
	client *Client
}

func createAuthExchange(
	client *Client, capacity uint16) (*AuthExchange, error) {

	result := &AuthExchange{client: client}
	err := result.exchange.init(
		"auth", "direct", result.client.conn, capacity,
		func(message string) { result.client.LogError(message) })
	if err != nil {
		return nil, err
	}
	return result, nil
}

// Close closes the exchange.
func (exchange *AuthExchange) Close() {
	exchange.exchange.close()
}

// CreateServer creates an authorization server to handle user authorization
// requests.
func (exchange *AuthExchange) CreateServer() (*AuthServer, error) {
	return createAuthServer(exchange)
}

// CreateServerOrExit creates an authorization server to handle user
// authorization or exits with error printing if creating is failed.
func (exchange *AuthExchange) CreateServerOrExit() *AuthServer {
	result, err := exchange.CreateServer()
	if err != nil {
		exchange.client.LogErrorf(`Failed to create auth-server: "%s".`, err)
		os.Exit(1)
	}
	return result
}

// CreateService creates an authorization service to request user authorization.
func (exchange *AuthExchange) CreateService() (*AuthService, error) {
	return createAuthService(exchange)
}

// CreateServiceOrExit creates an authorization service to request user
// authorization or exits with error printing if creating is failed.
func (exchange *AuthExchange) CreateServiceOrExit() *AuthService {
	result, err := exchange.CreateService()
	if err != nil {
		exchange.client.LogErrorf(`Failed to create auth-service: "%s".`, err)
		os.Exit(1)
	}
	return result
}

///////////////////////////////////////////////////////////////////////////////

// AuthServer represents server which handles authorization requests.
type AuthServer struct {
	rpcServer
}

func createAuthServer(exchange *AuthExchange) (*AuthServer, error) {
	result := &AuthServer{}
	err := result.rpcServer.init("auth", &exchange.exchange, exchange.client)
	if err != nil {
		return nil, err
	}
	return result, nil
}

// Close stops the server.
func (server *AuthServer) Close() {
	server.rpcServer.close()
}

// Handle accepts authorization requests and calls a handler for each.
func (server *AuthServer) Handle(
	handle func(login, password string) (*Auth, error)) {
	server.handle(func(message amqp.Delivery) (interface{}, error) {
		request := AuthRequest{}
		err := json.Unmarshal(message.Body, &request)
		if err != nil {
			server.client.LogErrorf(`Failed to parse auth-request "%s": "%s".`,
				string(message.Body), err)
			return nil, errors.New("Internal error")
		}

		var response *Auth
		response, err = handle(request.Login, request.Password)
		if err != nil {
			server.client.LogDebugf(
				`Failed to auth login "%s": "%s".`, request.Login, err)
			return nil, err
		}

		server.client.LogDebugf(`Login "%s" is authorized.`, request.Login)
		return response, nil
	})
}

///////////////////////////////////////////////////////////////////////////////

// AuthService represents service which accepts authorization requests and
// returns authorization result.
type AuthService struct {
	rpcClient
}

func createAuthService(exchange *AuthExchange) (*AuthService, error) {
	result := &AuthService{}
	err := result.rpcClient.init(&exchange.exchange, exchange.client)
	if err != nil {
		return nil, err
	}
	return result, nil
}

// Close stops the service.
func (service *AuthService) Close() {
	service.rpcClient.close()
}

// Request requests a user authorization.
func (service *AuthService) Request(
	request AuthRequest,
	handleSuccess func(Auth), handleFail func(error)) {
	service.rpcClient.request(
		"auth", // key
		true,   // mandatory
		request,
		func(response []byte) {
			result := Auth{}
			err := json.Unmarshal(response, &result)
			if err != nil {
				service.client.LogErrorf(`Failed to read response: "%s". Message: %s.`,
					err, string(response))
				handleFail(errors.New("Failed to read response"))
				return
			}
			handleSuccess(result)
		},
		handleFail)
}

///////////////////////////////////////////////////////////////////////////////
