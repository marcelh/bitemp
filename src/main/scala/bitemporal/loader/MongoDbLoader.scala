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

class MongoDbLoader(mongoDb: MongoDB) extends Loader with Logging {

    val DATA_COLLECTION = "data"
    val META_COLLECTION = "meta"
        
    def load(parser: Parser) {
    	val metaCol = mongoDb(META_COLLECTION)
    	val dataCol = mongoDb(DATA_COLLECTION)
    	val metaData = parser.metaData
    	if (isLoaded(metaCol, metaData("sha1")))
    	    logger.warn("Not loading " + metaData("sha1") + " as it is already known!")
	    else {
	        try {
	        	val metaRef = storeMeta(metaCol, metaData)
	        	while (parser.hasNext) {
	        	    storeRecord(dataCol, parser.next, metaRef)
	        	}
	        } finally {
	            parser.close
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
    }
    
    def isLoaded(collection: MongoCollection, sha1: Any): Boolean = {
		val q = MongoDBObject("sha1" -> sha1)
		collection.findOne(q).isDefined
    }
}