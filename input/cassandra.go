// Package input creates a benthos input
package input // nolint

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/Jeffail/benthos/v3/public/service"
)

var inputConfigSpec = service.NewConfigSpec().
	Summary("This input executes a Cassandra query and sends the records upstream").
	Field(service.NewObjectField(integrationsParam,
		service.NewStringField(serviceAddressParam))).
	Field(service.NewStringField(entityTypeParam)).
	Field(service.NewStringField(integrationIDParam)).
	Field(service.NewStringField(timeoutParam).Default("5s")).
	Field(service.NewIntField(maxInFlightParam).Default("1")).
	Field(service.NewObjectField(identityParam,
		service.NewStringField(serviceAddressParam),
		service.NewStringField(clientIDParam),
		service.NewStringField(clientSecretParam),
		service.NewStringListField(scopesParam)))

func init() {
	err := service.RegisterInput("cassandra", inputConfigSpec, newCassandraInput)
	if err != nil {
		panic(err)
	}
}

type integrationInfo struct {
	tenantID int64
	url      string
	user     string
	password string
}

type cassandraInput struct {
	entityType    string
	integrationID string
	client        *http.Client
	messageChan   chan *service.Message
	done          chan struct{}
	logger        *service.Logger
}

// nolint
func newCassandraInput(conf *service.ParsedConfig, mgr *service.Resources) (service.Input, error) {
	integrationServiceAddress, err := conf.Namespace(integrationsParam).FieldString(serviceAddressParam)
	if err != nil {
		return nil, fmt.Errorf("error retrieving %s.%s: %v", integrationsParam, serviceAddressParam, err)
	}
	integrationID, err := conf.FieldString(integrationIDParam)
	if err != nil {
		return nil, fmt.Errorf("error retrieving %s: %v", integrationIDParam, err)
	}
	entityType, err := conf.FieldString(entityTypeParam)
	if err != nil {
		return nil, fmt.Errorf("error retrieving %s: %v", entityTypeParam, err)
	}
	if entityType == "" {
		return nil, fmt.Errorf("%s can't be empty", entityTypeParam)
	}
	maxInfFlight, err := conf.FieldInt(maxInFlightParam)
	if err != nil {
		return nil, fmt.Errorf("error retrieving %s: %v", maxInFlightParam, err)
	}
	timeout, err := conf.FieldString(timeoutParam)
	if err != nil {
		return nil, fmt.Errorf("error retrieving %s: %v", timeoutParam, err)
	}
	identityAddress, err := conf.Namespace(identityParam).FieldString(serviceAddressParam)
	if err != nil {
		return nil, fmt.Errorf("error retrieving %s.%s: %v", identityParam, serviceAddressParam, err)
	}
	clientID, err := conf.Namespace(identityParam).FieldString(clientIDParam)
	if err != nil {
		return nil, fmt.Errorf("error retrieving %s.%s: %v", identityParam, clientIDParam, err)
	}
	clientSecret, err := conf.Namespace(identityParam).FieldString(clientSecretParam)
	if err != nil {
		return nil, fmt.Errorf("error retrieving %s.%s: %v", identityParam, clientSecretParam, err)
	}
	scopes, err := conf.Namespace(identityParam).FieldStringList(scopesParam)
	if err != nil {
		return nil, fmt.Errorf("error retrieving %s.%s: %v", identityParam, scopesParam, err)
	}
	client := &http.Client{}
	if timeout != "" {
		if client.Timeout, err = time.ParseDuration(timeout); err != nil {
			return nil, fmt.Errorf("failed to parse timeoutParam string: %v", err)
		}
	}
	token, err := getIdentityToken(identityAddress, clientID, clientSecret, scopes)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve token from identity: %v", err)
	}
	integration, err := getIntegrationInfo(integrationServiceAddress, integrationID, entityType, token)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve integration info: %v", err)
	}
	gs, err := scheduler.NewGDCScheduler(
		integration.tenantID,
		integration.url,
		model.UserPassword{
			User:     integration.user,
			Password: integration.password,
		},
	)
	if err != nil {
		return nil, err
	}
	input := cassandraInput{
		gdcScheduler:  gs,
		client:        client,
		integrationID: integrationID,
		entityType:    entityType,
		messageChan:   make(chan *service.Message, maxInfFlight),
		done:          make(chan struct{}),
		logger:        mgr.Logger(),
	}
	return &input, nil
}

//------------------------------------------------------------------------------

func (c *cassandraInput) Connect(ctx context.Context) error {
	go func() {
		defer func() {
			c.done <- struct{}{}
		}()
		msgs, err := c.GetMessagesFromCassandra()
		if err != nil {
			c.logger.Errorf(err.Error())
			return
		}
		for _, record := range msgs.Records {
			c.messageChan <- service.NewMessage(record)
		}
	}()
	return nil
}

func (c *cassandraInput) Read(ctx context.Context) (*service.Message, service.AckFunc, error) {
	select {
	case message := <-c.messageChan:
		return message, func(ctx context.Context, err error) error {
			return nil
		}, nil
	case <-c.done:
		return nil, nil, service.ErrEndOfInput
	}
}

func (c *cassandraInput) Close(ctx context.Context) error {
	return nil
}
