// Copyright 2018 REKTRA Network, All Rights Reserved.

package trekt

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
type AuthExchange struct{ mqExchange }

func createAuthExchange(trekt *Trekt, capacity uint16) (*AuthExchange, error) {

	result := &AuthExchange{}
	err := result.mqExchange.init("auth", "direct", trekt, capacity)
	if err != nil {
		return nil, err
	}
	return result, nil
}

// Close closes the exchange.
func (exchange *AuthExchange) Close() {
	exchange.mqExchange.close()
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
		exchange.trekt.LogErrorf(`Failed to create auth-server: "%s".`, err)
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
		exchange.trekt.LogErrorf(`Failed to create auth-service: "%s".`, err)
		os.Exit(1)
	}
	return result
}

///////////////////////////////////////////////////////////////////////////////

// AuthServer represents server which handles authorization requests.
type AuthServer struct {
	mqRPCServer
}

func createAuthServer(exchange *AuthExchange) (*AuthServer, error) {
	result := &AuthServer{}
	err := result.mqRPCServer.init("auth", &exchange.mqExchange)
	if err != nil {
		return nil, err
	}
	return result, nil
}

// Close stops the server.
func (server *AuthServer) Close() {
	server.mqRPCServer.close()
}

// Handle accepts authorization requests and calls a handler for each.
func (server *AuthServer) Handle(
	handle func(login, password string) (*Auth, error)) {

	server.handle(func(message amqp.Delivery) (interface{}, error) {
		request := AuthRequest{}
		err := json.Unmarshal(message.Body, &request)
		if err != nil {
			server.exchange.trekt.LogErrorf(
				`Failed to parse auth-request "%s": "%s".`,
				string(message.Body), err)
			return nil, errors.New("Internal error")
		}

		var response *Auth
		response, err = handle(request.Login, request.Password)
		if err != nil {
			server.exchange.trekt.LogDebugf(
				`Failed to auth login "%s": "%s".`, request.Login, err)
			return nil, err
		}

		server.exchange.trekt.LogDebugf(`Login "%s" is authorized.`, request.Login)
		return response, nil
	})
}

///////////////////////////////////////////////////////////////////////////////

// AuthService represents service which accepts authorization requests and
// returns authorization result.
type AuthService struct{ mqRPCClient }

func createAuthService(exchange *AuthExchange) (*AuthService, error) {
	result := &AuthService{}
	err := result.mqRPCClient.init(&exchange.mqExchange)
	if err != nil {
		return nil, err
	}
	return result, nil
}

// Close stops the service.
func (service *AuthService) Close() {
	service.mqRPCClient.close()
}

// Request requests a user authorization.
func (service *AuthService) Request(
	request AuthRequest,
	handleSuccess func(Auth), handleFail func(error)) {
	service.mqRPCClient.request(
		"auth", // key
		true,   // mandatory
		request,
		func(response []byte) {
			result := Auth{}
			err := json.Unmarshal(response, &result)
			if err != nil {
				service.exchange.trekt.LogErrorf(
					`Failed to read response: "%s". Message: %s.`,
					err, string(response))
				handleFail(errors.New("Failed to read response"))
				return
			}
			handleSuccess(result)
		},
		handleFail)
}

///////////////////////////////////////////////////////////////////////////////
