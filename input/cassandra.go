// Package input creates a benthos input
package input

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/Jeffail/benthos/v3/lib/message/batch"
	"github.com/Jeffail/benthos/v3/lib/util/retries"
	"github.com/gocql/gocql"

	"github.com/Jeffail/benthos/v3/public/service"
)

var inputConfigSpec = service.NewConfigSpec().
	Summary("This input executes a Cassandra query and pipes the resulting row values to upstream").
	Field(service.NewStringListField(addressesParam)).
	Field(service.NewObjectField(tlsParam,
		service.NewBoolField(enabledParam).Default(false),
		service.NewBoolField(skipCertVerifyParam),
		service.NewBoolField(enableRenegotiationParam),
	)).
	Field(service.NewObjectField(passwordAuthenticationParam,
		service.NewBoolField(enabledParam).Default(false),
		service.NewStringField(usernameParam).Default(""),
		service.NewStringField(passwordParam).Default(""),
	)).
	Field(service.NewBoolField(disableInitialHostLookupParam)).
	Field(service.NewStringField(keyspaceParam)).
	Field(service.NewIntField(pageSizeParam).Default(defaultPageSize)).
	Field(service.NewStringField(queryParam)).
	Field(service.NewStringListField(argsParam).Default([]string{})).
	Field(service.NewStringField(argsMappingParam).Default("")).
	Field(service.NewStringField(consistencyParam).Default("One")).
	Field(service.NewStringField(timeoutParam).Default("600ms")).
	Field(service.NewIntField(maxInFlightParam).Default("1")).
	Field(service.NewObjectField(batchingParam,
		service.NewIntField(countParam),
		service.NewIntField(byteSizeParam),
		service.NewStringField(periodParam)))

func init() {
	err := service.RegisterInput("cassandra", inputConfigSpec, newCassandraInput)
	if err != nil {
		panic(err)
	}
}

type cassandraInput struct {
	messageChan chan *service.Message
	done        chan struct{}

	conf    CassandraConfig
	tlsConf *tls.Config

	session    *gocql.Session
	backoffMin time.Duration
	backoffMax time.Duration

	connLock sync.RWMutex
	logger   *service.Logger
}

// nolint
func newCassandraInput(conf *service.ParsedConfig, mgr *service.Resources) (service.Input, error) {
	addresses, err := conf.FieldStringList(addressesParam)
	if err != nil {
		return nil, fmt.Errorf("error retrieving %s: %v", addressesParam, err)
	}
	tlsEnabled, err := conf.Namespace(tlsParam).FieldBool(enabledParam)
	if err != nil {
		return nil, fmt.Errorf("error retrieving %s.%s: %v", tlsParam, enabledParam, err)
	}
	skipCertVerify, err := conf.Namespace(tlsParam).FieldBool(skipCertVerifyParam)
	if err != nil {
		return nil, fmt.Errorf("error retrieving %s.%s: %v", tlsParam, skipCertVerifyParam, err)
	}
	enableRenegotiation, err := conf.Namespace(tlsParam).FieldBool(enableRenegotiationParam)
	if err != nil {
		return nil, fmt.Errorf("error retrieving %s.%s: %v", tlsParam, enableRenegotiationParam, err)
	}
	passwordAuthenticatorEnabled, err := conf.Namespace(passwordAuthenticationParam).FieldBool(enabledParam)
	if err != nil {
		return nil, fmt.Errorf("error retrieving %s.%s: %v", passwordAuthenticationParam, enabledParam, err)
	}
	username, err := conf.Namespace(passwordAuthenticationParam).FieldString(usernameParam)
	if err != nil {
		return nil, fmt.Errorf("error retrieving %s.%s: %v", passwordAuthenticationParam, usernameParam, err)
	}
	password, err := conf.Namespace(passwordAuthenticationParam).FieldString(passwordParam)
	if err != nil {
		return nil, fmt.Errorf("error retrieving %s.%s: %v", passwordAuthenticationParam, passwordParam, err)
	}
	disableInitialHostLookup, err := conf.FieldBool(disableInitialHostLookupParam)
	if err != nil {
		return nil, fmt.Errorf("error retrieving %s: %v", disableInitialHostLookupParam, err)
	}
	query, err := conf.FieldString(queryParam)
	if err != nil {
		return nil, fmt.Errorf("error retrieving %s: %v", queryParam, err)
	}
	args, err := conf.FieldStringList(argsParam)
	if err != nil {
		return nil, fmt.Errorf("error retrieving %s: %v", argsParam, err)
	}
	argsMapping, err := conf.FieldString(argsMappingParam)
	if err != nil {
		return nil, fmt.Errorf("error retrieving %s: %v", argsMappingParam, err)
	}
	consistency, err := conf.FieldString(consistencyParam)
	if err != nil {
		return nil, fmt.Errorf("error retrieving %s: %v", consistencyParam, err)
	}
	pageSize, err := conf.FieldInt(pageSizeParam)
	if err != nil {
		return nil, fmt.Errorf("error retrieving %s: %v", pageSizeParam, err)
	}
	keyspace, err := conf.FieldString(keyspaceParam)
	if err != nil {
		return nil, fmt.Errorf("error retrieving %s: %v", keyspaceParam, err)
	}

	timeout, err := conf.FieldString(timeoutParam)
	if err != nil {
		return nil, fmt.Errorf("error retrieving %s: %v", timeoutParam, err)
	}
	maxInfFlight, err := conf.FieldInt(maxInFlightParam)
	if err != nil {
		return nil, fmt.Errorf("error retrieving %s: %v", maxInFlightParam, err)
	}

	byteSize, err := conf.Namespace(batchingParam).FieldInt(byteSizeParam)
	if err != nil {
		return nil, fmt.Errorf("error retrieving %s.%s: %v", batchingParam, byteSizeParam, err)
	}
	count, err := conf.Namespace(batchingParam).FieldInt(countParam)
	if err != nil {
		return nil, fmt.Errorf("error retrieving %s.%s: %v", batchingParam, countParam, err)
	}
	period, err := conf.Namespace(batchingParam).FieldString(periodParam)
	if err != nil {
		return nil, fmt.Errorf("error retrieving %s.%s: %v", batchingParam, periodParam, err)
	}

	config := CassandraConfig{
		Addresses: addresses,
		Keyspace:  keyspace,
		TLS: TLSConfig{
			Enabled:             tlsEnabled,
			InsecureSkipVerify:  skipCertVerify,
			EnableRenegotiation: enableRenegotiation,
		},
		PasswordAuthenticator: PasswordAuthenticator{
			Enabled:  passwordAuthenticatorEnabled,
			Username: username,
			Password: password,
		},
		DisableInitialHostLookup: disableInitialHostLookup,
		Query:                    query,
		Args:                     args,
		ArgsMapping:              argsMapping,
		Consistency:              consistency,
		PageSize:                 pageSize,
		Timeout:                  timeout,
		Config: retries.Config{
			MaxRetries: 3,
			Backoff: retries.Backoff{
				InitialInterval: "1s",
				MaxInterval:     "5s",
				MaxElapsedTime:  "",
			},
		},
		MaxInFlight: maxInfFlight,
		Batching: batch.PolicyConfig{
			ByteSize: byteSize,
			Count:    count,
			Period:   period,
		},
	}

	input := cassandraInput{
		conf:        config,
		messageChan: make(chan *service.Message, maxInfFlight),
		done:        make(chan struct{}),
		logger:      mgr.Logger(),
	}

	if config.TLS.Enabled {
		if input.tlsConf, err = config.TLS.Get(); err != nil {
			return nil, err
		}
	}

	return &input, nil
}

//------------------------------------------------------------------------------

type decorator struct {
	NumRetries int
	Min, Max   time.Duration
}

func (d *decorator) Attempt(q gocql.RetryableQuery) bool {
	if q.Attempts() > d.NumRetries {
		return false
	}
	time.Sleep(getExponentialTime(d.Min, d.Max, q.Attempts()))
	return true
}

func getExponentialTime(min, max time.Duration, attempts int) time.Duration {
	minFloat := float64(min)
	napDuration := minFloat * math.Pow(2, float64(attempts-1)) //nolint:gomnd

	// Add some jitter
	napDuration += rand.Float64()*minFloat - (minFloat / 2) //nolint:gosec,gomnd
	if napDuration > float64(max) {
		return max
	}
	return time.Duration(napDuration)
}

func (d *decorator) GetRetryType(err error) gocql.RetryType {
	switch t := err.(type) {
	// not enough replica alive to perform query with required consistency
	case *gocql.RequestErrUnavailable:
		if t.Alive > 0 {
			return gocql.RetryNextHost
		}
		return gocql.Retry
	// write timeout - uncertain whatever write was successful or not
	case *gocql.RequestErrWriteTimeout:
		if t.Received > 0 {
			return gocql.Ignore
		}
		return gocql.Retry
	default:
		return gocql.Rethrow
	}
}

// ClientCertConfig contains config fields for a client certificate.
type ClientCertConfig struct {
	CertFile string `json:"cert_file" yaml:"cert_file"`
	KeyFile  string `json:"key_file" yaml:"key_file"`
	Cert     string `json:"cert" yaml:"cert"`
	Key      string `json:"key" yaml:"key"`
}

// PasswordAuthenticator contains the fields that will be used to authenticate with
// the Cassandra cluster.
type PasswordAuthenticator struct {
	Enabled  bool   `json:"enabled" yaml:"enabled"`
	Username string `json:"username" yaml:"username"`
	Password string `json:"password" yaml:"password"`
}

//------------------------------------------------------------------------------

// TLSConfig contains the TLS configuration
type TLSConfig struct {
	ClientCertificates  []ClientCertConfig `json:"client_certs" yaml:"client_certs"`
	RootCAs             string             `json:"root_cas" yaml:"root_cas"`
	RootCAsFile         string             `json:"root_cas_file" yaml:"root_cas_file"`
	Enabled             bool               `json:"enabled" yaml:"enabled"`
	InsecureSkipVerify  bool               `json:"skip_cert_verify" yaml:"skip_cert_verify"`
	EnableRenegotiation bool               `json:"enable_renegotiation" yaml:"enable_renegotiation"`
}

// Get returns a valid *tls.Config based on the configuration values of Config.
// If none of the config fields are set then a nil config is returned.
func (c *TLSConfig) Get() (*tls.Config, error) {
	var tlsConf *tls.Config
	initConf := func() {
		if tlsConf != nil {
			return
		}
		tlsConf = &tls.Config{
			MinVersion: tls.VersionTLS12,
		}
	}

	if len(c.RootCAs) > 0 && len(c.RootCAsFile) > 0 {
		return nil, errors.New("only one field between root_cas and root_cas_file can be specified")
	}

	if len(c.RootCAsFile) > 0 {
		caCert, err := os.ReadFile(c.RootCAsFile)
		if err != nil {
			return nil, err
		}
		initConf()
		tlsConf.RootCAs = x509.NewCertPool()
		tlsConf.RootCAs.AppendCertsFromPEM(caCert)
	}

	if len(c.RootCAs) > 0 {
		initConf()
		tlsConf.RootCAs = x509.NewCertPool()
		tlsConf.RootCAs.AppendCertsFromPEM([]byte(c.RootCAs))
	}

	for _, conf := range c.ClientCertificates {
		cert, err := conf.Load()
		if err != nil {
			return nil, err
		}
		initConf()
		tlsConf.Certificates = append(tlsConf.Certificates, cert)
	}

	if c.EnableRenegotiation {
		initConf()
		tlsConf.Renegotiation = tls.RenegotiateFreelyAsClient
	}

	if c.InsecureSkipVerify {
		initConf()
		tlsConf.InsecureSkipVerify = true
	}

	return tlsConf, nil
}

// Load returns a TLS certificate, based on either file paths in the
// config or the raw certs as strings.
func (c *ClientCertConfig) Load() (tls.Certificate, error) {
	if c.CertFile != "" || c.KeyFile != "" {
		if c.CertFile == "" {
			return tls.Certificate{}, errors.New("missing cert_file field in client certificate config")
		}
		if c.KeyFile == "" {
			return tls.Certificate{}, errors.New("missing key_file field in client certificate config")
		}
		return tls.LoadX509KeyPair(c.CertFile, c.KeyFile)
	}
	if c.Cert == "" {
		return tls.Certificate{}, errors.New("missing cert field in client certificate config")
	}
	if c.Key == "" {
		return tls.Certificate{}, errors.New("missing key field in client certificate config")
	}
	return tls.X509KeyPair([]byte(c.Cert), []byte(c.Key))
}

// CassandraConfig contains configuration fields for the Cassandra output type.
type CassandraConfig struct {
	Addresses                []string              `json:"addresses" yaml:"addresses"`
	Keyspace                 string                `json:"keyspace" yaml:"keyspace"`
	PageSize                 int                   `json:"page_size" yaml:"page_size"`
	TLS                      TLSConfig             `json:"tls" yaml:"tls"`
	PasswordAuthenticator    PasswordAuthenticator `json:"password_authenticator" yaml:"password_authenticator"`
	DisableInitialHostLookup bool                  `json:"disable_initial_host_lookup" yaml:"disable_initial_host_lookup"`
	Query                    string                `json:"query" yaml:"query"`
	Args                     []string              `json:"args" yaml:"args"`
	ArgsMapping              string                `json:"args_mapping" yaml:"args_mapping"`
	Consistency              string                `json:"consistency" yaml:"consistency"`
	Timeout                  string                `json:"timeout" yaml:"timeout"`
	retries.Config           `json:",inline" yaml:",inline"`
	MaxInFlight              int                `json:"max_in_flight" yaml:"max_in_flight"`
	Batching                 batch.PolicyConfig `json:"batching" yaml:"batching"`
}

//------------------------------------------------------------------------------

// ConnectToCassandra establishes a connection to Cassandra.
func (c *cassandraInput) ConnectToCassandra(ctx context.Context) error {
	c.connLock.Lock()
	defer c.connLock.Unlock()
	if c.session != nil {
		return nil
	}

	var err error
	cluster := gocql.NewCluster(c.conf.Addresses...)
	cluster.Keyspace = c.conf.Keyspace
	if c.tlsConf != nil {
		cluster.SslOpts = &gocql.SslOptions{
			Config: c.tlsConf,
			CaPath: c.conf.TLS.RootCAsFile,
		}
		cluster.DisableInitialHostLookup = c.conf.TLS.InsecureSkipVerify
	}
	if c.conf.PasswordAuthenticator.Enabled {
		cluster.Authenticator = gocql.PasswordAuthenticator{
			Username: c.conf.PasswordAuthenticator.Username,
			Password: c.conf.PasswordAuthenticator.Password,
		}
	}
	cluster.DisableInitialHostLookup = c.conf.DisableInitialHostLookup
	if cluster.Consistency, err = gocql.ParseConsistencyWrapper(c.conf.Consistency); err != nil {
		return fmt.Errorf("parsing consistency: %w", err)
	}

	cluster.RetryPolicy = &decorator{
		NumRetries: int(c.conf.Config.MaxRetries),
		Min:        c.backoffMin,
		Max:        c.backoffMax,
	}
	if tout := c.conf.Timeout; len(tout) > 0 {
		var perr error
		if cluster.Timeout, perr = time.ParseDuration(tout); perr != nil {
			return fmt.Errorf("failed to parse timeout string: %v", perr)
		}
	}
	session, err := cluster.CreateSession()
	if err != nil {
		return fmt.Errorf("creating Cassandra session: %w", err)
	}

	c.session = session
	c.session.SetConsistency(gocql.ParseConsistency(c.conf.Consistency))
	c.session.SetPageSize(c.conf.PageSize)
	c.logger.Infof("Getting messages from Cassandra: %v\n", c.conf.Addresses)
	return nil
}

func (c *cassandraInput) Connect(ctx context.Context) error {
	if err := c.ConnectToCassandra(ctx); err != nil {
		return err
	}
	go func() {
		var iter *gocql.Iter
		defer func() {
			c.done <- struct{}{}
			if err := iter.Close(); err != nil {
				c.logger.Errorf("query failed: %v", err)
			}
		}()
		var (
			more = true
			page []byte
		)
		for more {
			iter = c.session.Query(c.conf.Query).WithContext(context.Background()).PageSize(c.conf.PageSize).PageState(page).Iter()
			page = iter.PageState()
			if len(page) == 0 {
				return
			}
			m := make(map[string]interface{})
			for iter.MapScan(m) {
				j, err := json.Marshal(m)
				if err != nil {
					c.logger.Errorf("creating cassandra session: %w", err)
					return
				}
				c.messageChan <- service.NewMessage(j)
			}
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
