package bitemporal.loader
import java.io.File
import org.joda.time.Interval

/**
 * A batch is essentially a sequence of Records.
 */
trait Record {
    
    /**
     * Returns the identifier for this record
     */
    def recordId: String
    
    /**
     * Returns the interval for which this record is valid
     */
    def validInterval: Interval
    
    /**
     * Returns the key-value pairs
     */
    def values: Map[String, Any]
}

/**
 * Trait for some kind of input.
 */
trait Batch {

    /**
     * Returns the meta-data for the input (if any)
     */
    def metaData: Map[String, String]
    
    /**
     * Returns the unique identifier for this input (for example the md5sum of the entire file)
     */
    def batchId: String
    
    /**
     * Returns the name of this input (for example the path name of the file)
     */
    def name: String
    
    /**
     * Returns an iterator for the BatchRecords
     */
    def recordIterator: Iterator[Record]
}