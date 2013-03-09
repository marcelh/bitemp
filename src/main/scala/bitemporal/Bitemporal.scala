package bitemporal

import org.joda.time.DateTime
import org.joda.time.Interval
import org.joda.time.DateTimeZone.UTC

/**
 * Bi-temporal trait with the two time dimensions.
 */
trait Bitemporal {
    def trxTimestamp: DateTime
    def validInterval: Interval
}

object BitemporalEntity {
    val keyLoaderId = "loader_id"
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
		
    /* *
     * Returns true if this entity can be merged with that entity.
     * Two entities can be merged when:
     *   - their ids are the same
     *   - the values are equal
     *   - the valid intervals abuts or overlap
     * 
     * @param that the entity to compare with
     * @return true if it can be merged, false if not
     * /
    def isMergeableWith(that: BitemporalEntity): Boolean = {
	    this.id == that.id &&
	    this.values == that.values &&
	    overlapsOrAbutsWith(that.validInterval)
	}
	*/
}

