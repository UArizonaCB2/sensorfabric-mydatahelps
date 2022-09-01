package commons

import (
	"archive/zip"
	"os"
	"sensorfabric/mydatahelps/databases"
	"sensorfabric/mydatahelps/mdevices"
	"strings"
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
				if mdevice.CheckFile(file.Name) {
					mdevice.Process(file, &db)
				}
			}
		}
	}

	return nil
}
