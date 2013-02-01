package bitemporal.store

import java.util.concurrent.TimeUnit.MILLISECONDS
import java.util.concurrent.TimeUnit.SECONDS
import org.joda.time.DateTime
import org.joda.time.Interval
import com.mongodb.casbah.Imports.MongoClient
import com.mongodb.casbah.Imports.MongoDB
import com.mongodb.casbah.Imports.wrapDBObj
import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.commons.conversions.scala.RegisterJodaTimeConversionHelpers
import com.typesafe.config.Config
import com.yammer.metrics.scala.Instrumented
import bitemporal.BitemporalEntity
import bitemporal.BitemporalStore
import com.mongodb.casbah.MongoCollection
import com.mongodb.casbah.MongoConnection
import com.weiglewilczek.slf4s.Logging
import com.mongodb.casbah.query.Implicits._
import com.mongodb.DBObject
import com.mongodb.casbah.Imports._


case class BitemporalMongoDbEntity(
		id: String,
		allValues: Map[String, Any],
        trxTimestamp: DateTime,
        validInterval: Interval
    ) extends BitemporalEntity {
    
    val specialKeys = Set("_id", "id", "valid-from", "valid-until", "tx")
	def values: Map[String, Any] = allValues filter { case (k, v) => !specialKeys.contains(k) }
}

class BitemporalMongoDbStore(val config: Config) 
		extends BitemporalStore with Instrumented with MongoControl with Logging {
    
    import com.mongodb.casbah.commons.conversions.scala.RegisterJodaTimeConversionHelpers
    RegisterJodaTimeConversionHelpers()
    
    val getTiming = metrics.timer("get-time")
    val putTiming = metrics.timer("put-time")

    def get(id: String, 
            validAt: DateTime = DateTime.now, 
            asOf: DateTime = DateTime.now): Option[BitemporalEntity] = { 
        
        logger.debug("get(%s, validAt=%s, asOf=%s)".format(id, validAt, asOf))
        
        getTiming.time {
            usingMongo { conn =>
		        val qry: DBObject = ("tx" $lte asOf) ++ 
		        					("valid-from" $lte validAt) ++ 
		        					("valid-until" $gt validAt) ++
		        					("id" -> id)
		        val cursor = dataCollection(conn).find(qry).sort(MongoDBObject("tx" -> -1)).limit(1)
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
    
    def put(id: String,
            values: Map[String, Any], 
            validInterval: Interval): BitemporalEntity = {
        
        logger.debug("put(%s, %s, %s)".format(id, values, validInterval))
        
		// TODO merge valid interval with existing entities
        
    		
		def interval2map(iv: Interval): Map[String, Any] = {
				Map("valid-from" -> iv.getStart(), "valid-until" -> iv.getEnd())
		}
		
		putTiming.time {
	        val txT = DateTime.now
	        val extendedValues = values ++ interval2map(validInterval) + ("tx" -> txT) + ("id" -> id)
	        val mongoObj = MongoDBObject(extendedValues.toList)
	        usingMongo { conn => 
	        	dataCollection(conn) += mongoObj
	        }
	        
			BitemporalMongoDbEntity(id, values, txT, validInterval)
		}
    }
    
    private def toEntity(obj: MongoDBObject): BitemporalEntity = {
        val id = obj.getAs[String]("id").get
        val values = obj.map { case (k,v) => (k -> v)} .toMap
        val tx = obj.getAs[DateTime]("tx").get
        val start = obj.getAs[DateTime]("valid-from").get
        val end   = obj.getAs[DateTime]("valid-until").get
        BitemporalMongoDbEntity(id, values, tx, new Interval(start, end))
    }
}