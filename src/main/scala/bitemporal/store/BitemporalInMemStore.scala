package bitemporal.store

import bitemporal._
import bitemporal.BitemporalStore.{startOfTime,endOfTime}
import org.joda.time.{DateTime,Interval}

/**
 * A BitemporalStore that keeps all data in memory.
 * Data is never removed but simply prepended to a list so that the latest additions are at the front.
 * Searching is just a linear search through the list. Merging of entities with respect to the valid interval is done
 * at retrieval time. Putting a new entity in the store is simply prepending the entity with the given valid interval to
 * the internal list. So putting is cheap, retrieving is more expensive.
 */
class BitemporalInMemStore extends BitemporalStore {
    
    // the list should be kept in reverse transaction time order
    var entities = List.empty[BitemporalEntity]

	/**
	 * Search for the first matching entity given the id, validAt and asOf time stamp in list of entities. 
	 * This first matched entity is then merged with all entities having the same id and values.
	 * 
	 * @param id the entity id to search
	 * @param validAt the time stamp at which the entity must be valid
	 * @param asOf the time stamp at which the entity must have been available in the store
	 * @return the merged entity if any
	 */
	def get(id: String, 
            validAt: DateTime = DateTime.now, 
            asOf: DateTime = DateTime.now): Option[BitemporalEntity] =
    {
		def matches(entity: BitemporalEntity): Boolean = {
				(entity.trxTimestamp.isBefore(asOf) || entity.trxTimestamp.isEqual(asOf)) &&
				entity.validInterval.contains(validAt) &&
				entity.id == id
		}
		
		def mergeable(e1: BitemporalEntity, e2: BitemporalEntity): Boolean = {
		    e1.id == e2.id &&
		    (e1.trxTimestamp.isBefore(asOf) || e1.trxTimestamp.isEqual(asOf)) &&
		    e1.values == e2.values &&
		    (e1.validInterval.abuts(e2.validInterval) || e1.validInterval.overlaps(e2.validInterval))
		}
		
		def mergeInterval(i1: Interval, i2: Interval): Interval = {
		    new Interval(
		            math.min(i1.getStartMillis(), i2.getStartMillis()), 
		            math.max(i1.getEndMillis(), i2.getEndMillis()))
		}
		
		def mergeEntities(e1: BitemporalEntity, e2: BitemporalEntity): BitemporalEntity = {
		    if (mergeable(e1, e2))
		        InMemBitemporalEntity(e1.id, 
		                e1.values, 
		                mergeInterval(e1.validInterval, e2.validInterval), 
		                e1.trxTimestamp)
		    else
		        e1
		}
		
		def mergeValidInterval(entity: BitemporalEntity, store: List[BitemporalEntity]): BitemporalEntity = {
		    store match {
		        case Nil => entity
		        case head :: tail => mergeValidInterval(mergeEntities(entity, head), tail)
		    }
		}
		
        val first = entities find (matches(_))
        if (first.isDefined)
        	Some(mergeValidInterval(first.get, entities))
        else
        	None
    }
    
    /**
     * Simply create a new entity with the given properties and prepend it to the internal entity list.
     * 
     * @param id the entity id
     * @param values the set of values for this entity
     * @param validInterval the interval at which this entity is valid
     * @return the stored entity
     */
    def put(id: String,
            values: Map[String, Any],
            validInterval: Interval = new Interval(startOfTime,endOfTime)): BitemporalEntity = {
        val newRecord = InMemBitemporalEntity(id, values, validInterval, new DateTime)
        entities = newRecord :: entities
        newRecord
    }
    
    override def toString = {
        val sb = new StringBuilder()
        sb.append(getClass.getSimpleName).append("{\n")
        entities foreach(e => sb.append("  ").append(e).append('\n'))
        sb.append('}')
        sb.toString
    }
}

case class InMemBitemporalEntity(
        id: String,
        values: Map[String, Any],
    	validInterval: Interval,
    	trxTimestamp: DateTime = new DateTime
    ) extends BitemporalEntity

