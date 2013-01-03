package bitemporal.store

import bitemporal.BitemporalStore
import org.joda.time.DateTime
import bitemporal.BitemporalEntity
import bitemporal.AttributeValue
import org.joda.time.Interval
import com.mongodb.casbah.Imports._

class BitemporalMongoDbStore extends BitemporalStore {

    val mongoConn = MongoConnection()
    
    def get(id: String, 
            validAt: DateTime = DateTime.now, 
            asOf: DateTime = DateTime.now): Option[BitemporalEntity] = { None }
    
    def put(id: String,
            values: Set[AttributeValue], 
            validInterval: Interval): BitemporalEntity = null
}