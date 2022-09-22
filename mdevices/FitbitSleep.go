/*
 * This file contains the fitbit module for parsing MyDataHelps file that are specific to fitbit intraday heart rate.
 */

package mdevices

import (
	"archive/zip"
	"bufio"
	"encoding/json"
	"sensorfabric/mydatahelps/databases"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
)

type FitbitSleep struct {
	fileTag string // Stores the fitbit filetag used to identify activity MyDataHelps file.
}

func CreateFitbitSleep(fileTag string) *FitbitSleep {
	return &FitbitSleep{fileTag: fileTag}
}

func (f FitbitSleep) CheckFile(filename string) bool {
	return strings.Contains(filename, f.fileTag)
}

/*
 * Method which reads the contents of the activity file and then inserts it the database.
 */
func (f FitbitSleep) Process(file *zip.File, db databases.GeneralDatabase) (err error) {
	logger := log.WithFields(log.Fields{
		"package":  "modules",
		"function": "FitbitSleep",
	})

	logger.Println("Processing file " + file.Name)

	// These csv files are not split according to participant ID. It is just one single file.
	fileName := file.Name
	fileName += ".csv" // Tags don't contain file extensions, so we add them.

	// Open the file for reading
	fopen, err := file.Open()
	if err != nil {
		logger.Errorf("Process Aborted : Error (%s) while opening the file %s", err.Error(), fileName)
		return err
	}
	defer fopen.Close()

	var buffer [][]byte
	// Test code to see buffered reads.
	scanner := bufio.NewScanner(fopen)
	// Skip the frst line, which are the column headings.

	for scanner.Scan() {
		line := scanner.Text()
		linbuff := strings.Split(line, ",")
		if len(linbuff) < 6 {
			continue
		}

		_map := make(map[string]interface{})
		_map[participantID] = linbuff[0]
		_map["logDate"], _ = time.Parse("2006-01-02T15:04:05", linbuff[1])
		_map["startDate"], _ = time.Parse("2006-01-02T15:04:05", linbuff[3])
		_map["endDate"], _ = time.Parse("2006-01-02T15:04:05", linbuff[4])
		_map["type"] = linbuff[2]
		_map["value"] = linbuff[5]

		// Marshal the data.
		_buffer, err := json.Marshal(_map)
		if err != nil {
			logger.Warnf("Failed to unmarshal value with error %s", err.Error())
		}

		// TODO: Handle the failure case when _map filed to unmarhsal. _buffer is in inconsistent state.
		buffer = append(buffer, _buffer)
	}

	logger.Infof("%d sleep records found", len(buffer))

	// We are finally ready to dump all these values into the database.
	if len(buffer) > 0 {
		err = db.InsertRaw("fitbit_sleep", buffer)
		if err != nil {
			logger.Errorf("Error inserting into database.\nError : %s", err.Error())
			return err
		}
	}

	return nil
}

func (f FitbitSleep) GetName() string {
	return "FitbitSleep"
}
