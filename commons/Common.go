package commons

import (
	"archive/zip"
	"os"
	"sensorfabric/mydatahelps/databases"
	"sensorfabric/mydatahelps/mdevices"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"

	log "github.com/sirupsen/logrus"
)

// An array of device modules that scan through the list of files and perform
// device specific database operations.
// Scope : local
var devices []mdevices.GenericDevice

/*
 * Setup all the wearable M-Devices (Mobile Health Devices)
 */
func Setup() {
	devices = []mdevices.GenericDevice{
		mdevices.CreateFitbitActivityHeartRate("fitbit_intraday_activities_heart"),
	}
}

/*
 * Top level method which starts scanning the given directory for MyDataHelps
 * format exported .zip files.
 * exportDir : Path to the export directory where the .zip files are.
 */
func Looper(exportDir string, db databases.GeneralDatabase) (err error) {
	logger := log.WithFields(log.Fields{
		"module":   "commons",
		"function": "Looper",
	})

	if len(exportDir) <= 0 {
		// Raise an error.
	}

	files, err := os.ReadDir(exportDir)
	for _, file := range files {
		/*
		 * The format of the MyDataHelps export files is specific and as follows -
		 * RK.[ID].[Project Name]_[Start Date : YYYYMMDD]-[End Date : YYYYMMDD].zip
		 */
		fileName := file.Name()
		fileName = strings.ToLower(fileName)
		buff := strings.Split(fileName, ".")
		if len(buff) != 4 {
			// Ignore this file as it is not the correct format.
			continue
		}

		if buff[len(buff)-1] != "zip" {
			// This file does not have the correct extension.
			continue
		}

		// Attempt to open the zip file. Will error out if the file is not the correct format.
		result, err := zip.OpenReader(exportDir + "/" + fileName)
		if err != nil {
			// Errored out, do not process this file.
			continue
		}

		defer result.Close()

		// Iterate through all the files in the zip and pass them to the correct m-devices.
		for _, file := range result.File {
			for _, mdevice := range devices {
				// Check the collection "processed" to see if have already processed this file for the given m-device.
				filter := bson.M{
					"$and": []bson.M{
						bson.M{"file": file.Name}, // We use the full file path in the zip file.
						bson.M{"module": mdevice.GetName()},
					},
				}
				result, err := db.ReadOne("processed", filter)
				if err != mongo.ErrNoDocuments {
					// We found a document matching that, hence we have already processed this and we continue.
					logger.Warnf("Skipping %s for %s as it has already been processed %v", file.Name, mdevice.GetName(), result["date"])
					continue
				}
				if mdevice.CheckFile(file.Name) {
					if err = mdevice.Process(file, db); err == nil {
						// Make sure to add this in the processed collection as we have successfuly processed it.
						document := bson.M{
							"file":   file.Name,
							"module": mdevice.GetName(),
							"date":   time.Now(),
						}
						// TODO : If this errors out we try to keep adding the data into the collection until timeout.
						db.InsertOne("processed", document)
					}
				}
			}
		}
	}

	return nil
}
