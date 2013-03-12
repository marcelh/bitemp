package bitemporal.loader

import com.yammer.metrics.scala.Instrumented

import bitemporal.BitemporalEntity.keyLoaderId
import bitemporal.BitemporalRepository
import grizzled.slf4j.Logging

/**
 * Loader trait for bi-temporal data.
 */
trait Loader extends Instrumented with Logging {

    /**
     * The repository where the (bi-temporal) data will be stored.
     */
    def bitempRepository: BitemporalRepository

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
        
        def storeRecord(record: Record, loaderId: Any) {
            val values = record.values + (keyLoaderId -> loaderId)
        	bitempRepository.put(record.recordId, values, record.validInterval)
            recordsCounting.mark
        }
        
        loadTiming.time {
	        loaderRepository.putOrGet(batch.batchId, batch.metaData) match {
	            case Right(ent) =>
		            logger.info("Loading batch '" + ent.id + "' ...")
		            batch.recordIterator.foreach(r => storeRecord(r, ent.id))
	            case Left(ent) =>
		        	logger.warn("Batch '" + ent.id + "' already loaded!")
	        }
        }
        
        recordsCounting.count
    }
}

