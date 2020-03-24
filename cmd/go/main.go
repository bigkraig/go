package main

import (
	"context"
	"fmt"
	"log"
	"strings"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"

	"github.com/kellegous/go/backend"
	"github.com/kellegous/go/backend/dynamodb"
	"github.com/kellegous/go/backend/firestore"
	"github.com/kellegous/go/backend/leveldb"
	"github.com/kellegous/go/web"
)

func main() {
	pflag.String("addr", ":8067", "default bind address")
	pflag.Bool("admin", false, "allow admin-level requests")
	pflag.String("version", "", "version string")
	pflag.String("backend", "leveldb", "backing store to use. 'leveldb', 'dynamodb' and 'firestore' currently supported.")
	pflag.String("data", "data", "The location of the leveldb data directory")
	pflag.String("project", "", "The GCP project to use for the firestore backend. Will attempt to use application default creds if not defined.")

	pflag.String("table", "", "The table to use for the DynamoDB backend.")

	pflag.Parse()

	if err := viper.BindPFlags(pflag.CommandLine); err != nil {
		log.Panic(err)
	}

	// allow env vars to set pflags
	viper.AutomaticEnv()
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))

	var backend backend.Backend

	switch viper.GetString("backend") {
	case "leveldb":
		var err error
		backend, err = leveldb.New(viper.GetString("data"))
		if err != nil {
			log.Panic(err)
		}
	case "firestore":
		var err error

		backend, err = firestore.New(context.Background(), viper.GetString("project"))
		if err != nil {
			log.Panic(err)
		}
	case "dynamodb":
		var err error

		backend, err = dynamodb.New(context.Background(), viper.GetString("table"))
		if err != nil {
			log.Panic(err)
		}
	default:
		log.Panic(fmt.Sprintf("unknown backend %s", viper.GetString("backend")))
	}

	defer backend.Close()

	log.Panic(web.ListenAndServe(backend))
}
