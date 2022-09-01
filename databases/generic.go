/*
 * An interface that specifies general datbase connection and common methods
 * that each database connector must support.
 */

package databases

type GeneralDatabase interface {
	/*
	 * Method used to enter the given array of json-byte representation into the
	 * specified collection / table. This method is expected to perform batch insertion operations.
	 * table : The table (RDS) or collection (NoSQL) to insert into.
	 * jsonBytes : A 2 dimensional array when each row containing a json byte representation to be inserted.
	 */
	Insert(table string, jsonBytes [][]byte) (err error)
	/*
	* Connect to the database.
	 */
	Connect() error
	/*
	* Disconnect from the database
	 */
	Disconnect() error
}
