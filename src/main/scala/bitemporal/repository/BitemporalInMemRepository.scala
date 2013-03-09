package bitemporal.repository

import org.joda.time.DateTime
import org.joda.time.Interval

import bitemporal.BitemporalEntity
import bitemporal.BitemporalRepository
import bitemporal.BitemporalRepository.endOfTime
import bitemporal.BitemporalRepository.startOfTime


case class InMemBitemporalEntity(
        id: String,
        values: Map[String, Any],
    	validInterval: Interval,
    	trxTimestamp: DateTime = new DateTime
    ) extends BitemporalEntity

/**
 * A BitemporalRepository that keeps all data in memory.
 * Data is never removed but simply prepended to a list so that the latest additions are at the front.
 * Searching is just a linear search through the list. Merging of entities with respect to the valid interval is done
 * at retrieval time. Putting a new entity in the store is simply prepending the entity with the given valid interval to
 * the internal list. So putting is cheap, retrieving is more expensive.
 */
class BitemporalInMemRepository extends BitemporalRepository {
    
    // the list should be kept in reverse transaction time order
    var entities = List.empty[BitemporalEntity]

	/**
	 * Search for the first matching entity given the id, validAt and asOf time stamp in list of entities. 
	 * 
	 * @param id the entity id to search
	 * @param validAt the time stamp at which the entity must be valid
	 * @param asOf the time stamp at which the entity must have been available in the store
	 * @return the entity if any
	 */
	def get(id: String, 
            validAt: DateTime = DateTime.now, 
            asOf: DateTime = DateTime.now): Option[BitemporalEntity] =
    {
        entities find ( _.matches(id, validAt, asOf) )
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
