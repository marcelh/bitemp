package bitemporal.repository.mongo

import org.joda.time.DateTime
import org.joda.time.Interval

import com.mongodb.DBObject
import com.mongodb.casbah.Imports._
import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.commons.conversions.scala.RegisterJodaTimeConversionHelpers
import com.typesafe.config.Config
import com.yammer.metrics.scala.Instrumented

import bitemporal.BitemporalEntity
import bitemporal.BitemporalRepository
import grizzled.slf4j.Logging


case class BitemporalMongoDbEntity(
		id: String,
		values: Map[String, Any],
        validInterval: Interval,
        knownAt: DateTime
    ) extends BitemporalEntity

object BitemporalMongoDbRepository {
	val KEY_ID = "id"
	val KEY_VALID_FROM = "valid-from"
	val KEY_VALID_UNTIL = "valid-until"
	val KEY_KNOWN_AT = "known-at"
	    
	lazy val SPECIAL_KEYS = Set(KEY_ID, KEY_VALID_FROM, KEY_VALID_UNTIL, KEY_KNOWN_AT)
}

class BitemporalMongoDbRepository(val config: Config) 
		extends BitemporalRepository with Instrumented with MongoControl with Logging {
    
    RegisterJodaTimeConversionHelpers()
    
    import bitemporal.repository.mongo.BitemporalMongoDbRepository._
    
    val getTiming = metrics.timer("get-time")
    val getRangeTiming = metrics.timer("get-range-time")
    val putTiming = metrics.timer("put-time")

    /** Convert MongoDB object to BitemporalEntity */
    private def toEntity(obj: MongoDBObject): BitemporalEntity = {
    	val id = obj.getAs[String](KEY_ID).get
		val values = obj.map { case (k,v) => (k -> v)} .toMap
		val knownAt = obj.getAs[DateTime](KEY_KNOWN_AT).get
		val validStart = obj.getAs[DateTime](KEY_VALID_FROM).get
		val validEnd   = obj.getAs[DateTime](KEY_VALID_UNTIL).get
		BitemporalMongoDbEntity(id, values, new Interval(validStart, validEnd), knownAt)
    }
    
    def get(id: String, validAt: DateTime, asOf: DateTime): Option[BitemporalEntity] = {
        logger.debug(s"get(id=$id, validAt=$validAt, asOf=$asOf)")
        getTiming.time {
            usingMongo { conn =>
		        val qry: DBObject = (KEY_KNOWN_AT $lte asOf) ++ 
		        					(KEY_VALID_FROM $lte validAt) ++ 
		        					(KEY_VALID_UNTIL $gt validAt) ++
		        					(KEY_ID -> id)
		        val cursor = bitempCollection(conn).find(qry).sort(MongoDBObject(KEY_KNOWN_AT -> -1)).limit(1)
				if (cursor.hasNext) {
				    val ent = toEntity(cursor.next)
				    logger.debug("get result: " + ent)
				    Some(ent)
				}
			    else
			        None
            }
        }
    }
    
    def get(id: String, asOfInterval: Interval): Seq[BitemporalEntity] = {
        logger.debug(s"get(id=$id, asOfInterval=$asOfInterval)")
        getTiming.time {
            usingMongo { conn =>
		        val qry: DBObject = (KEY_KNOWN_AT $gte asOfInterval.getStart $lt asOfInterval.getEnd) ++ (KEY_ID -> id)
		        val cursor = bitempCollection(conn).find(qry).sort(MongoDBObject(KEY_KNOWN_AT -> -1)).limit(500)
		        cursor.map(m => toEntity(m)).toSeq
            }
        }
    }
    
    /**
     * New entries are simply put as-is in MongoDB mapping the valid-interval to the fields 'valid-from' and 
     * 'valid-until' and with the known-at time stamp set to 'now'.
     */
    def put(id: String, 
            values: Map[String, Any], 
            validInterval: Interval, 
            knownAt: DateTime = DateTime.now): BitemporalEntity =
    {
        logger.debug(s"put(id=$id, values=$values, validInterval=$validInterval, knownAt=$knownAt)")
        
		def interval2map(iv: Interval): Map[String, Any] = {
				Map(KEY_VALID_FROM -> iv.getStart(), KEY_VALID_UNTIL -> iv.getEnd())
		}
		
		putTiming.time {
	        val extendedValues = values ++ interval2map(validInterval) + (KEY_KNOWN_AT -> knownAt) + (KEY_ID -> id)
	        val mongoObj = MongoDBObject(extendedValues.toList)
	        usingMongo { conn => 
	        	bitempCollection(conn) += mongoObj
	        }
	        
			BitemporalMongoDbEntity(id, values, validInterval, knownAt)
		}
    }
}