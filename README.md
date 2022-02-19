# Benthos custom Cassandra input plugin

This custom benthos plugin executes a cassandra query and forwards the resulting 
row values to the upstream pipeline
It can be configured through the following env vars:

| Env Var                                    | Description | Default value          |
|--------------------------------------------|-------------|------------------------|
| HTTP_ADDRESS                               |             | 0.0.0.0:4195           |
| CASSANDRA_ADDRESS                          |             | localhost              |
| CASSANDRA_PORT                             |             | 9042                   |
| CASSANDRA_TLS_ENABLED                      |             | true                   |
| CASSANDRA_TLS_SKIP_CERT_VERIFY             |             | true                   |
| CASSANDRA_TLS_ENABLE_RENEGOTIATION         |             | true                   |
| CASSANDRA_PASSWORD_AUTHENTICATOR_ENABLED   |             | true                   |
| CASSANDRA_USER                             |             |                        |
| CASSANDRA_PASSWORD                         |             |                        |
| CASSANDRA_DISABLE_INITIAL_HOST_LOOKUP      |             | true                   |
| CASSANDRA_KEYSPACE                         |             | my_keyspace            |
| CASSANDRA_QUERY                            |             | select * from my_table |
| CASSANDRA_PAGE_SIZE                        |             | 100                    |
| CASSANDRA_TIMEOUT                          |             | 1s                     |
| CASSANDRA_MAX_IN_FLIGHT                    |             | 1                      |
| CASSANDRA_OUTPUT_BATCH_COUNT               |             | 0                      |
| CASSANDRA_OUTPUT_BATCH_BYTE_SIZE           |             | 0                      |
| CASSANDRA_OUTPUT_BATCH_PERIOD              |             |                        |

The response row values will be automatically marshaled to json.

