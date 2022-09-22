/*
 * This file contains the fitbit module for parsing MyDataHelps file that are specific to fitbit intraday heart rate.
 */

package mdevices

import (
	"archive/zip"
	"bufio"
	"encoding/json"
	"errors"
	"sensorfabric/mydatahelps/databases"
	"strings"
	"time"

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
func (f FitbitActivityHeartRate) Process(file *zip.File, db databases.GeneralDatabase) (err error) {
	logger := log.WithFields(log.Fields{
		"package":  "modules",
		"function": "FitbitActivityHeartRate",
	})

	logger.Println("Processing file " + file.Name)

	// Isolate the participant ID from the path.
	fileName := file.Name
	pathBuf := strings.Split(fileName, "/")
	if len(pathBuf) != 3 {
		logger.Errorln("Process Aborted : \nLength mismatch. Expecting fitbit_intraday_activities_heart/<participant id>/<json file name> \nbut found " + fileName)
		return errors.New("length error")
	}
	participantID := pathBuf[1]

	// Open the file for reading
	fopen, err := file.Open()
	if err != nil {
		logger.Errorf("Process Aborted : Error (%s) while opening the file %s", err.Error(), fileName)
		return err
	}
	defer fopen.Close()

	// Since these are regular files, and have no holes in them we can safely assume the size of the file
	// corresponds to the actual data inside the file.
	fileSize := file.FileInfo().Size()

	// Test code to see buffered reads.
	bufferedReader := bufio.NewReader(fopen)
	byteBuffer := make([]byte, fileSize)
	bytesRead := 0
	for temp, _ := bufferedReader.Read(byteBuffer); temp > 0; {
		bytesRead = bytesRead + temp
		temp, _ = bufferedReader.Read(byteBuffer[bytesRead:])
	}

	if err != nil {
		logger.Errorf("Process Aborted : %s", err.Error())
		return err
	}
	if bytesRead < int(fileSize) {
		logger.Warnf("Possible data loss. Bytes written to cache is not equal to the size of file.\n File Size = %d, Bytes Read = %d", fileSize, bytesRead)
	}

	// Unrmashall the json-bytes into a map.
	var dataMap map[string]interface{}
	if err := json.Unmarshal(byteBuffer, &dataMap); err != nil {
		logger.Errorf("Process Aborted : JSON Unmarshall failed with the following error %s", err.Error())
		return err
	}

	var startUnixDate int64
	activitiesHeart := dataMap["activities-heart"].([]interface{})
	for _, act := range activitiesHeart {
		temp := act.(map[string]interface{})
		startDate, _ := time.Parse("2006-01-02", temp["dateTime"].(string))
		startUnixDate = startDate.Unix()
	}

	hrValues := dataMap["activities-heart-intraday"].(map[string]interface{})
	dataSet := hrValues["dataset"].([]interface{})

	logger.Infof("%d heart rate values found", len(dataSet))

	buffer := make([][]byte, len(dataSet))

	for idx, data := range dataSet {
		temp := data.(map[string]interface{})
		_time := temp["time"].(string)
		_value := temp["value"].(float64)

		_dateObj, _ := time.Parse("15:04:05", _time)
		_timeOffset := _dateObj.Hour()*60*60 + _dateObj.Minute()*60 + _dateObj.Second()

		_map := make(map[string]interface{})
		_map["time"] = int64(_timeOffset) + startUnixDate
		_map["hr"] = _value
		_map["participantID"] = participantID

		// Marshal the data.
		_buffer, err := json.Marshal(_map)
		if err != nil {
			logger.Warnf("Failed to unmarshal value with error %s", err.Error())
		}

		// TODO: Handle the failure case when _map filed to unmarhsal. _buffer is in inconsistent state.
		buffer[idx] = _buffer
	}

	// We are finally ready to dump all these values into the database.
	if len(buffer) > 0 {
		err = db.InsertRaw("intraday_heartrate", buffer)
		if err != nil {
			logger.Errorf("Error inserting into database.\nError : %s", err.Error())
			return err
		}
	}

	return nil
}

func (f FitbitActivityHeartRate) GetName() string {
	return "FitbitActivityHeartRate"
}
