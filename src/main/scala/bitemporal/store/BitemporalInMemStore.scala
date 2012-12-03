package bitemporal.store

import bitemporal._
import org.joda.time.{DateTime,Interval}

/**
 * A BitemporalStore that keeps all data in memory.
 * Data is never removed but simply prepended to a list so that the latest additions are at the front.
 * Searching is just a linear search through the list. The first match is returned as result.
 */
class BitemporalInMemStore extends BitemporalStore {
    
    // the list should be kept in reverse transaction time order
    var data = List.empty[BitemporalEntity]

	def get(id: String, 
            validAt: DateTime = DateTime.now, 
            asOf: DateTime = DateTime.now): Option[BitemporalEntity] =
    {
		def matches(entity: BitemporalEntity): Boolean = {
				(entity.trxTimestamp.isBefore(asOf) || entity.trxTimestamp.isEqual(asOf)) &&
				entity.validInterval.contains(validAt) &&
				entity.id == id
		}
		
        data find (matches(_))
    }
    
    def put(id: String,
            values: Set[AttributeValue],
            validInterval: Interval = new Interval(startOfTime,endOfTime)): BitemporalEntity = {
        val valuesMap = values.map(av => (av.attribute.id -> av)).toMap
        val newRecord = InMemBitemporalEntity(id, valuesMap, validInterval, new DateTime)
        data = newRecord :: data
        newRecord
    }
    
    override def toString = {
        val sb = new StringBuilder()
        sb.append(getClass.getSimpleName).append("{\n")
        data foreach(e => sb.append("  ").append(e).append('\n'))
        sb.append('}')
        sb.toString
    }
}

case class InMemBitemporalEntity(
        id: String,
        values: Map[String, AttributeValue],
    	validInterval: Interval,
    	trxTimestamp: DateTime = new DateTime
    ) extends BitemporalEntity

