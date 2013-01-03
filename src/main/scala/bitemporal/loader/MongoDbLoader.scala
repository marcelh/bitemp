package bitemporal.loader
import java.io.File
import java.io.FileInputStream
import java.security.MessageDigest
import scala.Array.canBuildFrom
import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.MongoDB
import com.mongodb.casbah.MongoCollection
import com.weiglewilczek.slf4s.Logging
import org.bson.types.ObjectId

class MongoDbLoader(mongoDb: MongoDB) extends Logging {

    val BATCH_ID = "batch-id"
    val DATA_COLLECTION = "data"
    val META_COLLECTION = "meta"
        
    def load(parser: BatchParser) {
    	val metaCol = mongoDb(META_COLLECTION)
    	val dataCol = mongoDb(DATA_COLLECTION)
    	val metaData = parser.metaData
    	if (isLoaded(metaCol, parser.identifier))
    	    logger.warn("Not loading batch '" + parser.identifier + "' as it is already known!")
	    else {
	    	val metaRef = storeMeta(metaCol, metaData + (BATCH_ID->parser.identifier))
	        parser.processData(record => storeRecord(dataCol, record, metaRef))
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
    }
    
    def isLoaded(collection: MongoCollection, identifier: Any): Boolean = {
		val q = MongoDBObject(BATCH_ID -> identifier)
		collection.findOne(q).isDefined
    }
}