/*
 * An interface that outlines the methods supported by devices - fitbit, apple watch, etc.
 */

package mdevices

import (
	"archive/zip"
	"sensorfabric/mydatahelps/databases"
)

const participantID = "participantID"

type GenericDevice interface {
	CheckFile(filename string) bool
	Process(file *zip.File, db databases.GeneralDatabase) error
	GetName() string
}
