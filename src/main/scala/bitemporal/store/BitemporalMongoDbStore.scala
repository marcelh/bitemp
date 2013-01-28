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

case class BitemporalMongoDbEntity(
		id: String,
		values: Map[String, Any],
        trxTimestamp: DateTime,
        validInterval: Interval
    ) extends BitemporalEntity
        
class BitemporalMongoDbStore(val config: Config) extends BitemporalStore with Instrumented with MongoControl {
    
    import com.mongodb.casbah.commons.conversions.scala.RegisterJodaTimeConversionHelpers
    RegisterJodaTimeConversionHelpers()
    
    val getTiming = metrics.timer("get-time")
    val putTiming = metrics.timer("put-time")

    def get(id: String, 
            validAt: DateTime = DateTime.now, 
            asOf: DateTime = DateTime.now): Option[BitemporalEntity] = { 
        
        getTiming.time {
            usingMongo { conn =>
		        val qry = MongoDBObject("id" -> id)
		        val cursor = dataCollection(conn).find(qry)
		        val matching = for {
		        	obj <- cursor
		            if obj.containsField("valid-from")
		            if validAt.isAfter(obj("valid-from").asInstanceOf[DateTime])
		            if obj.containsField("valid-until")
		            if validAt.isBefore(obj("valid-until").asInstanceOf[DateTime])
		            if obj.containsField("tx")
		            if asOf.isAfter(obj("tx").asInstanceOf[DateTime])
		        } yield obj
				if (matching.hasNext)
				    Some(toEntity(matching.next))
			    else
			        None
            }
        }
    }
    
    def put(id: String,
            values: Map[String, Any], 
            validInterval: Interval): BitemporalEntity = {
        
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
        val values = Map[String,Any]()
        val tx = obj.getAs[DateTime]("tx").get
        val start = obj.getAs[DateTime]("valid-from").get
        val end   = obj.getAs[DateTime]("valid-until").get
        BitemporalMongoDbEntity(id, values, tx, new Interval(start, end))
    }
}