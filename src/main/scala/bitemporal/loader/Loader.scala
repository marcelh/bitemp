package bitemporal.loader

import bitemporal.BitemporalRepository
import java.io.File
import java.util.concurrent.TimeUnit.{MILLISECONDS,SECONDS}
import com.yammer.metrics.reporting.ConsoleReporter
import com.yammer.metrics.scala.Instrumented
import com.yammer.metrics.annotation.Timed
import com.yammer.metrics.annotation.Metered
import com.weiglewilczek.slf4s.Logging

/**
 * Loader trait for bi-temporal data.
 */
trait Loader extends Instrumented with Logging {

    /**
     * The store (repository) where the data will be stored.
     */
    def bitempStore: BitemporalRepository

    /**
     * The repository containing information about loaded files.
     */
    def loaderRepository: LoaderRepository

    /**
     * Perform load operation, giving regular progress feedback by calling the process function.
     *
     * @param batch the batch to load
     * @return number of records loaded in case of success
     */
    def load(batch: Batch): Long = {
        val loadTiming = metrics.timer("load-time")
        val recordsCounting = metrics.meter("records-count", "records")
        
        def storeRecord(record: Record, metaDataRef: Any) {
            val values = record.values + ("meta_ref" -> metaDataRef)
        	bitempStore.put(record.recordId, values, record.validInterval)
            recordsCounting.mark
        }
        
        loadTiming.time {
	        loaderRepository.putOrGet(batch.batchId, batch.metaData) match {
	            case Right(ent) =>
		            val metaDataRef = ent.data("_id")
		            logger.info("Loading batch '" + ent.id + "' ...")
		            batch.recordIterator.foreach(r => storeRecord(r, metaDataRef))
	            case Left(ent) =>
		        	logger.warn("Batch '" + ent.id + "' already loaded!")
	        }
        }
        
        recordsCounting.count
    }
}

