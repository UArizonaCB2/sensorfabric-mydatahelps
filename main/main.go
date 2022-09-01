package main

import (
	"context"
	"encoding/json"
	"os"
	"sensorfabric/mydatahelps/commons"
	"sensorfabric/mydatahelps/databases"

	log "github.com/sirupsen/logrus"
)

func main() {
	log.SetLevel(log.DebugLevel)

	args := os.Args
	if len(args) < 2 {
		log.Fatal("Usage : go run server.go <path to configuration file>")
	}

	// Read the configuration file.
	contents, err := os.ReadFile(args[1])
	if err != nil {
		log.Fatal("Error reading configuration file. " + err.Error())
	}

	// Map that holds values from the configuration json file.
	var config map[string]interface{}
	if err = json.Unmarshal(contents, &config); err != nil {
		log.Fatal("Error decoding json " + err.Error())
	}

	// Create the mongodb database object.
	mongoUri := config["mongoUri"].(string)
	mongoName := config["mongoName"].(string)

	mongodb := databases.NewMongoConnection(context.Background(), mongoUri, mongoName)
	if err = mongodb.Connect(); err != nil {
		log.Fatal(err.Error())
	}

	// Scan the export directory to start ingesting the data.
	exportDir := config["exportDir"].(string)
	commons.Setup()
	commons.Looper(exportDir, mongodb)

	defer mongodb.Disconnect()
}
