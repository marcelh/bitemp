package bitemporal.store

import bitemporal._
import org.joda.time.{DateTime,Interval}

/**
 * A BitemporalStore that keeps all data in memory.
 * Data is never removed but simply prepended to a list so that the latest additions are at the front.
 * Searching is just a linear search through the list. The first match is returned as result.
 */
class BitemporalInMemStore[V] extends BitemporalStore[V] {
    
    // the list should be kept in reverse transaction time order
    var data = List.empty[Entity[V]]

	def get(id: String, 
            validAt: DateTime = DateTime.now, 
            asOf: DateTime = DateTime.now): Option[Entity[V]] =
    {
		def matches(entity: Entity[V]): Boolean = {
				(entity.trxTimestamp.isBefore(asOf) || entity.trxTimestamp.isEqual(asOf)) &&
				entity.validInterval.contains(validAt) &&
				entity.id == id
		}
		
        data find (matches(_))
    }
    
    def put(id: String,
            value: V,
            validInterval: Interval = new Interval(startOfTime,endOfTime)): Entity[V] = {
        val newRecord = Entity(id, value, validInterval, new DateTime)
        data = newRecord :: data
        newRecord
    }
    
    def dump = {
        val sb = new StringBuilder()
        sb.append(getClass.getSimpleName).append("{\n")
        data foreach(e => sb.append("  ").append(e).append('\n'))
        sb.append('}')
        sb.toString
    }
}
