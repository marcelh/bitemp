package bitemporal.repository

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
        trxTimestamp: DateTime,
        validInterval: Interval
    ) extends BitemporalEntity

class BitemporalMongoDbRepository(val config: Config) 
		extends BitemporalRepository with Instrumented with MongoControl with Logging {
    
    RegisterJodaTimeConversionHelpers()
    
    val getTiming = metrics.timer("get-time")
    val getRangeTiming = metrics.timer("get-range-time")
    val putTiming = metrics.timer("put-time")

    /** Convert MongoDB object to BitemporalEntity */
    private def toEntity(obj: MongoDBObject): BitemporalEntity = {
    	val id = obj.getAs[String]("id").get
		val values = obj.map { case (k,v) => (k -> v)} .toMap
		val tx = obj.getAs[DateTime]("tx").get
		val start = obj.getAs[DateTime]("valid-from").get
		val end   = obj.getAs[DateTime]("valid-until").get
		BitemporalMongoDbEntity(id, values, tx, new Interval(start, end))
    }
    
    def get(id: String, validAt: DateTime, asOf: DateTime): Option[BitemporalEntity] = {
        logger.debug(s"get(id=$id, validAt=$validAt, asOf=$asOf)")
        getTiming.time {
            usingMongo { conn =>
		        val qry: DBObject = ("tx" $lte asOf) ++ 
		        					("valid-from" $lte validAt) ++ 
		        					("valid-until" $gt validAt) ++
		        					("id" -> id)
		        val cursor = bitempCollection(conn).find(qry).sort(MongoDBObject("tx" -> -1)).limit(1)
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
		        val qry: DBObject = ("tx" $gte asOfInterval.getStart $lt asOfInterval.getEnd) ++ ("id" -> id)
		        val cursor = bitempCollection(conn).find(qry).sort(MongoDBObject("tx" -> -1)).limit(500)
		        cursor.map(m => toEntity(m)).toSeq
            }
        }
    }
    
    /**
     * New entries are simply put as-is in MongoDB mapping the valid-interval to the fields 'valid-from' and 
     * 'valid-until' and with the transaction time stamp set to 'now'.
     */
    def put(id: String, values: Map[String, Any], validInterval: Interval): BitemporalEntity = {
    	val txT = DateTime.now
        logger.debug(s"put(id=$id, values=$values, validInterval=$validInterval, tx=$txT)")
        
		def interval2map(iv: Interval): Map[String, Any] = {
				Map("valid-from" -> iv.getStart(), "valid-until" -> iv.getEnd())
		}
		
		putTiming.time {
	        val extendedValues = values ++ interval2map(validInterval) + ("tx" -> txT) + ("id" -> id)
	        val mongoObj = MongoDBObject(extendedValues.toList)
	        usingMongo { conn => 
	        	bitempCollection(conn) += mongoObj
	        }
	        
			BitemporalMongoDbEntity(id, values, txT, validInterval)
		}
    }
    
}