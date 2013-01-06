package bitemporal.loader

import java.util.concurrent.TimeUnit

import org.bson.types.ObjectId

import com.mongodb.casbah.MongoCollection
import com.mongodb.casbah.MongoDB
import com.mongodb.casbah.commons.MongoDBObject
import com.weiglewilczek.slf4s.Logging
import com.yammer.metrics.reporting.ConsoleReporter
import com.yammer.metrics.scala.Instrumented

class MongoDbLoader(mongoDb: MongoDB) extends Logging  with Instrumented {

	val loadTiming = metrics.timer("load-time", "single-file", TimeUnit.MILLISECONDS, TimeUnit.SECONDS)
	val recordsCounting = metrics.meter("records-count", "records", "single-file", TimeUnit.SECONDS)
	
    val BATCH_ID = "batch-id"
    val DATA_COLLECTION = "data"
    val META_COLLECTION = "meta"
        
    def load(parser: BatchParser) {
        loadTiming.time {
	    	val metaCol = mongoDb(META_COLLECTION)
	    	val dataCol = mongoDb(DATA_COLLECTION)
	    	val metaData = parser.metaData
	    	if (isLoaded(metaCol, parser.identifier))
	    	    logger.warn("Not loading '" + parser.name + "' as it is already known!")
		    else {
		    	val metaRef = storeMeta(metaCol, metaData + (BATCH_ID->parser.identifier))
		        parser.processData(record => storeRecord(dataCol, record, metaRef))
		    }
        }
    }
    
    def storeMeta(collection: MongoCollection, meta: Map[String, Any]): ObjectId = {
        val metaObj = MongoDBObject(meta.toList)
        collection += metaObj
        metaObj.get("_id").asInstanceOf[ObjectId]
    }
    
    def storeRecord(collection: MongoCollection, record: Map[String, Any], metaRef: ObjectId) {
        val recordObj = MongoDBObject(("meta_ref" -> metaRef) :: record.toList)
        collection += recordObj
        recordsCounting.mark()
    }
    
    def isLoaded(collection: MongoCollection, identifier: Any): Boolean = {
		val q = MongoDBObject(BATCH_ID -> identifier)
		collection.findOne(q).isDefined
    }
}