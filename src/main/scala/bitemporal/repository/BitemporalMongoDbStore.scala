package bitemporal.repository

import org.joda.time.DateTime
import org.joda.time.Interval

import com.mongodb.DBObject
import com.mongodb.casbah.Imports.JodaDateTimeDoNOk
import com.mongodb.casbah.Imports.mongoQueryStatements
import com.mongodb.casbah.Imports.wrapDBObj
import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.commons.conversions.scala.RegisterJodaTimeConversionHelpers
import com.typesafe.config.Config
import com.weiglewilczek.slf4s.Logging
import com.yammer.metrics.scala.Instrumented

import bitemporal.BitemporalEntity
import bitemporal.BitemporalRepository


case class BitemporalMongoDbEntity(
		id: String,
		values: Map[String, Any],
        trxTimestamp: DateTime,
        validInterval: Interval
    ) extends BitemporalEntity

class BitemporalMongoDbStore(val config: Config) 
		extends BitemporalRepository with Instrumented with MongoControl with Logging {
    
    RegisterJodaTimeConversionHelpers()
    
    val getTiming = metrics.timer("get-time")
    val putTiming = metrics.timer("put-time")

    def get(id: String, 
            validAt: DateTime = DateTime.now, 
            asOf: DateTime = DateTime.now): Option[BitemporalEntity] = { 
        
    	/* Convert MongoDB object to BitemporalEntity */
    	def toEntity(obj: MongoDBObject): BitemporalEntity = {
    			val id = obj.getAs[String]("id").get
    					val values = obj.map { case (k,v) => (k -> v)} .toMap
    					val tx = obj.getAs[DateTime]("tx").get
    					val start = obj.getAs[DateTime]("valid-from").get
    					val end   = obj.getAs[DateTime]("valid-until").get
    					BitemporalMongoDbEntity(id, values, tx, new Interval(start, end))
    	}
    	
        logger.debug("get(%s, validAt=%s, asOf=%s)".format(id, validAt, asOf))
        
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
    
    /* 
     * new entries are simply put as-is in MongoDB
     */
    def put(id: String,
            values: Map[String, Any], 
            validInterval: Interval): BitemporalEntity = {
        
        logger.debug("put(%s, %s, %s)".format(id, values, validInterval))
        
		def interval2map(iv: Interval): Map[String, Any] = {
				Map("valid-from" -> iv.getStart(), "valid-until" -> iv.getEnd())
		}
		
		putTiming.time {
	        val txT = DateTime.now
	        val extendedValues = values ++ interval2map(validInterval) + ("tx" -> txT) + ("id" -> id)
	        val mongoObj = MongoDBObject(extendedValues.toList)
	        usingMongo { conn => 
	        	bitempCollection(conn) += mongoObj
	        }
	        
			BitemporalMongoDbEntity(id, values, txT, validInterval)
		}
    }
    
}