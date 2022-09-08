/*
 * Database class that is used to connect to Mongodb.
 */

package databases

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	log "github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Mongo struct {
	connectionURI string
	client        *mongo.Client
	backgroundCtx context.Context // Background context.
	databaseName  string          // Name of the database
	database      *mongo.Database // Stores the database handle.
}

/*
* Constructor method that returns a Mongodb object.
 */
func NewMongoConnection(backgroundCtx context.Context, connectionURI string, databaseName string) *Mongo {
	return &Mongo{
		connectionURI: connectionURI,
		backgroundCtx: backgroundCtx,
		databaseName:  databaseName,
	}
}

/*
 * This methods establishes a new database connection.
 * connectionURI : A valid mongo db URI. For example "mongodb://localhost:27017"
 */
func (db *Mongo) Connect() (err error) {
	db.connectionURI = db.connectionURI

	if err = db.checkBackgroundContext(); err != nil {
		return err
	}

	db.client, err = mongo.NewClient(options.Client().ApplyURI(db.connectionURI))
	if err != nil {
		return err
	}

	// Create a 10 second timeout content, so the background go routine
	// for connection is terminated after a timeout.
	ctx, _ := context.WithTimeout(db.backgroundCtx, 10*time.Second)
	err = db.client.Connect(ctx)
	if err != nil {
		return err
	}

	db.database = db.client.Database(db.databaseName)

	return nil
}

/*
 * Method which closes the database connection.
 */
func (db *Mongo) Disconnect() (err error) {

	if err = db.checkBackgroundContext(); err != nil {
		return err
	}

	if err = db.checkClient(); err != nil {
		return err
	}

	ctx, _ := context.WithTimeout(db.backgroundCtx, 5*time.Second)

	return db.client.Disconnect(ctx)
}

/*
 * Method which checks if the background context has been set.
 */
func (db *Mongo) checkBackgroundContext() (err error) {
	if db.backgroundCtx == nil {
		return errors.New("Background context for DB Object cannot be nill")
	}

	return nil
}

/*
 * Method which checks if the mongo client has been set.
 * Does not gaurentee an active connection.
 */
func (db *Mongo) checkClient() (err error) {
	if db.client == nil {
		return errors.New("Database client cannot be nil. Make sure the connection has been established")
	}

	return nil
}

func (db *Mongo) InsertOne(collection string, value map[string]interface{}) (err error) {
	logger := log.WithFields(log.Fields{
		"package":  "databases",
		"function": "InsertOne",
	})

	buffer := make([][]byte, 1)
	buffer[0], err = json.Marshal(value)
	if err != nil {
		logger.Errorf("Failed to unmarshal value with error %s", err.Error())
		return err
	}

	return db.InsertRaw(collection, buffer)
}

/*
 * Method which inserts a document into the given collection.
 * collection : MongoDB collection to add this object into.
 * jsonBytes : A 2 dimensional array of bytes, where each row represents a set of bytes that can be converted to json.
 */
func (db *Mongo) InsertRaw(collection string, jsonBytes [][]byte) (err error) {
	logger := log.WithFields(log.Fields{
		"package":  "databases",
		"function": "Insert",
	})

	// Unmarshal the json bytes into key-value map.
	buffer := make([]interface{}, len(jsonBytes))
	bidx := 0
	for _, element := range jsonBytes {
		// We need to create a new variable, (memory allocation) since bson.M is tied to it (&temp).
		// If we don't we waste 1 hour debugging why all the elements in the buffer array are the same .....
		// Offcourse not saying I did that .....
		var temp map[string]interface{}
		if err := json.Unmarshal(element, &temp); err != nil {
			logger.Errorln("Faled to unmarshal JSON " + err.Error())
		} else {
			buffer[bidx] = bson.M(temp)
			bidx++
		}
	}

	// Do a batch insert into the database.
	result, err := db.database.Collection(collection).InsertMany(db.backgroundCtx, buffer)
	if err != nil {
		logger.Errorln("Error batch inserting into the Mongo database " + err.Error())
		return err
	}

	// Make sure that all the documents have been inserted.
	if len(result.InsertedIDs) != len(jsonBytes) {
		logger.Warnf("Possible Data Loss : Not all entries have been inserted into the database. %d Inserted but %d were requested.", len(result.InsertedIDs), len(jsonBytes))
	}

	return nil
}

/*
* Method which reads a single record from the database and returns it.
 * collection : MongoDB collection to read objects from.
 * filter: query to filter the results. A nil is same as .findOne({})
*/
func (db *Mongo) ReadOne(collection string, filter interface{}) (result map[string]interface{}, err error) {
	if filter == nil {
		filter = bson.D{}
	}

	if err = db.database.Collection(collection).FindOne(db.backgroundCtx, filter).Decode(&result); err != nil {
		return nil, err
	}

	return result, nil
}
