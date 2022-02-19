// main package for importer application
package main

import (
	"github.com/Jeffail/benthos/v3/lib/service"
	_ "github.com/mfamador/benthos-input-cassandra/input"
	"github.com/rs/zerolog/log"
)

func main() {
	log.Info().Msg("Starting cassandra importer")

	service.Run()
}
