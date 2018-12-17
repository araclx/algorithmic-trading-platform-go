// Copyright 2018 2018 REKTRA Network, All Rights Reserved.

package mqclient

import (
	"encoding/json"
	"errors"
	"log"
)

const authRPCServiceName = "auth"

///////////////////////////////////////////////////////////////////////////////

// Auth provides authorization information.
type Auth struct {
	Login string
}

///////////////////////////////////////////////////////////////////////////////

// AuthExchange represents authorization exchange.
type AuthExchange struct {
	exchange
	client *Client
}

func createAuthExchange(client *Client) (*AuthExchange, error) {
	result := &AuthExchange{client: client}
	err := result.exchange.init(
		"auth", "direct", result.client.conn,
		func(message string) { result.client.LogWarn(message) })
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
// authorization, or exit with error printing if creating is failed.
func (exchange *AuthExchange) CreateServerOrExit() *AuthServer {
	result, err := exchange.CreateServer()
	if err != nil {
		log.Fatalf(`Failed to create auth-server: "%s".`, err)
	}
	return result
}

// CreateService creates an authorization service to request user authorization.
func (exchange *AuthExchange) CreateService() (*AuthService, error) {
	return createAuthService(exchange)
}

// CreateServiceOrExit creates an authorization service to request user
// authorization, or exit with error printing if creating is failed.
func (exchange *AuthExchange) CreateServiceOrExit() *AuthService {
	result, err := exchange.CreateService()
	if err != nil {
		log.Fatalf(`Failed to create auth-service: "%s".`, err)
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
	err := result.rpcServer.init(authRPCServiceName, &exchange.exchange, exchange.client)
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
	server.handle(
		func(request []byte) ([]byte, error) {

			creds := struct {
				Login    string
				Password string
			}{}
			err := json.Unmarshal(request, &creds)
			if err != nil {
				server.client.LogErrorf(`Failed to parse auth-request "%s": "%s".`,
					string(request), err)
				return nil, errors.New("Internal error")
			}

			var auth *Auth
			auth, err = handle(creds.Login, creds.Password)
			if err != nil {
				server.client.LogDebugf(
					`Failed to auth login "%s": "%s".`, creds.Login, err)
				return nil, err
			}

			var response []byte
			response, err = json.Marshal(auth)
			if err != nil {
				server.client.LogErrorf(
					`Failed to serialize auth-response "%s": "%s".`,
					auth, string(request))
				return nil, errors.New("Internal error")
			}

			server.client.LogDebugf(`Login "%s" is authorized.`, creds.Login)
			return response, nil
		})
}

///////////////////////////////////////////////////////////////////////////////

// AuthService represents service which accepts authorization requests and
// returns authorization result.
type AuthService struct {
	rpcService
}

func createAuthService(exchange *AuthExchange) (*AuthService, error) {
	result := &AuthService{}
	err := result.rpcService.init(authRPCServiceName, &exchange.exchange, exchange.client)
	if err != nil {
		return nil, err
	}
	return result, nil
}

// Close stops the service.
func (service *AuthService) Close() {
	service.rpcService.close()
}

// Request requests a user authorization.
func (service *AuthService) Request(
	login, password string,
	handleSuccess func(Auth), handleFail func(error)) {
	service.rpcService.request(
		[]byte(`{"login":"`+login+`","password":"`+password+`"}`),
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
