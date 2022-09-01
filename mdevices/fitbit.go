/*
 * This file contains the fitbit module for parsing MyDataHelps file that are specific to fitbit.
 */

package mdevices

import (
	"archive/zip"
	"errors"
	"io"
	"os"
	"sensorfabric/mydatahelps/databases"
	"strings"

	log "github.com/sirupsen/logrus"
)

type FitbitActivityHeartRate struct {
	fileTag string // Stores the fitbit filetag used to identify activity MyDataHelps file.
}

func CreateFitbitActivityHeartRate(fileTag string) *FitbitActivityHeartRate {
	return &FitbitActivityHeartRate{fileTag: fileTag}
}

func (f FitbitActivityHeartRate) CheckFile(filename string) bool {
	return strings.Contains(filename, f.fileTag)
}

/*
 * Method which reads the contents of the activity file and then inserts it the database.
 */
func (f FitbitActivityHeartRate) Process(file *zip.File, db *databases.GeneralDatabase) (err error) {
	logger := log.WithFields(log.Fields{
		"package":  "modules",
		"function": "FitbitActivity",
	})

	logger.Println("Processing file " + file.Name)

	// Isolate the participant ID from the path.
	fileName := file.Name
	pathBuf := strings.Split(fileName, "/")
	if len(pathBuf) != 3 {
		logger.Errorln("Process Aborted : \nLength mismatch. Expecting fitbit_intraday_activities_heart/<participant id>/<json file name> \nbut found " + fileName)
		return errors.New("length error")
	}
	//participantID := pathBuf[1]

	// Open the file for reading
	fopen, err := file.Open()
	if err != nil {
		logger.Errorf("Process Aborted : Error (%s) while opening the file %s", err.Error(), fileName)
		return err
	}
	defer fopen.Close()

	// Since these are regular files, and have no holes in them we can safely assume the size of the file
	// corresponds to the actual data inside the file.
	//fileSize := file.FileInfo().Size()

	cacheFile, _ := os.OpenFile("/tmp/"+pathBuf[2], os.O_WRONLY|os.O_CREATE, os.ModePerm)
	defer cacheFile.Close()
	bytesWritten, err := io.Copy(cacheFile, fopen)
	logger.Printf("Bytes Written %d", bytesWritten)

	return nil
}

func (f FitbitActivityHeartRate) GetName() string {
	return "FitbitActivity"
}
