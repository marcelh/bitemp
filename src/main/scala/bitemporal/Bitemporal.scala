package bitemporal

import org.joda.time.{DateTime,Interval}
import org.joda.time.DateTimeZone.UTC

/**
 * Bi-temporal trait with the two time dimensions.
 */
trait Bitemporal {
    def trxTimestamp: DateTime
    def validInterval: Interval
}

/**
 * A bi-temporal entity with the two time dimensions, an unique entity identifier and a map with attribute identifier 
 * to attribute value.
 */
trait BitemporalEntity extends Bitemporal {
    def id: String
    def values: Map[String, Any]
    
    /**
     * Returns true if this entity existed on the asOf time-stamp
     * 
     * @param asOf time-stamp to compare against trxTimestamp
     * @return true if it existed, false if not
     */
    def existsAt(asOf: DateTime): Boolean = trxTimestamp.isBefore(asOf) || trxTimestamp.isEqual(asOf)
    
    /**
     * Returns true is this entity is/was valid on the given time-stamp
     *  
     * @param t time-stamp to compare against validInterval
     * @return true if t is contained in validInterval, false if not
     */
    def isValidAt(t: DateTime): Boolean = validInterval.contains(t)
    
    /**
     * Returns true if this entity matches with the given id, validAt and asOf.
     * 
     * @param id to compare against this.id
     * @param validAt should be included in this.validInterval
     * @param asOf should be equal or after this.trxTimestamp
     * @return true if it matches, false if not
     */
    def matches(id: String, validAt: DateTime, asOf: DateTime): Boolean = 
		existsAt(asOf) && isValidAt(validAt) && this.id == id
    
	/**
	 * Returns true if this entity validInterval overlaps or abuts (lie adjacent).
	 * 
	 * @param interval the other interval
	 * @return true if it overlaps or abuts, false if not
	 */
	def overlapsOrAbutsWith(interval: Interval): Boolean = 
        this.validInterval.abuts(interval) || this.validInterval.overlaps(interval)
		
    /**
     * Returns true if this entity can be merged with that entity.
     * Two entities can be merged when:
     *   - their ids are the same
     *   - the values are equal
     *   - the valid intervals abuts or overlap
     * 
     * @param that the entity to compare with
     * @return true if it can be merged, false if not
     */
    def isMergeableWith(that: BitemporalEntity): Boolean = {
	    this.id == that.id &&
	    this.values == that.values &&
	    overlapsOrAbutsWith(that.validInterval)
	}
}

object BitemporalStore {
    /** The smallest possible time stamp that can be stored */ 
    val startOfTime = new DateTime(0, 1, 1, 0, 0, UTC)
    /** The largest possible time stamp that can be stored */
    val endOfTime = new DateTime(9999, 12, 31, 23, 59, UTC)
}

/**
 * An interface to store bi-temporal entities
 */
trait BitemporalStore {

    /**
     * Get the entity valid on and known at the specified times. 
     * <p>
     * More formally: find the entity which matches the given id, has a validInterval that contains the given validAt 
     * time-stamp, has a trxTimestamp time-stamp that is the same or is before the given asOf time-stamp AND there 
     * exists no other entity which matches the same conditions but has an asOf between the first found entity 
     * and the given trxTimestamp time-stamp.
     * 
     * @param id the entity id (should not be null or empty)
     * @param validAt at which time the entity should be valid
     * @param asOf at which time the entity should have been in the store
     * @return Some[Entity] or None when no such entity exists in the store
     */
    def get(id: String, 
            validAt: DateTime = DateTime.now, 
            asOf: DateTime = DateTime.now): Option[BitemporalEntity]
    
    /**
     * Modify and/or add an entity with the given value to the store.
     * <p>
     * Depending on the implementation entities could be split, merged or created.
     * The returned entity might have a validInterval that is larger than specified.
     * 
     * @param id the entity id (should not be null or empty)
     * @param values the attribute values for the entity
     * @return the created entity
     */
    def put(id: String,
            values: Map[String, Any], 
            validInterval: Interval): BitemporalEntity
}

