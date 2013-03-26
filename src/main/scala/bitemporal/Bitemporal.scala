package bitemporal

import org.joda.time.DateTime
import org.joda.time.Interval
import org.joda.time.DateTimeZone.UTC

/**
 * Bi-temporal trait with the two time dimensions; the known at time-stamp and the valid time (interval).
 */
trait Bitemporal {
    
    /** Time interval for which this entity is valid. */
    def validInterval: Interval
    
    /** Time-stamp at which this entity became known (in the repository) */
    def knownAt: DateTime
}

/**
 * A bi-temporal entity with the two time dimensions, an unique entity identifier and a map with attribute identifier 
 * to attribute value.
 * <p>
 * Note that we need the identifier because we going to have multiple versions (instances) of the same entity in our 
 * repository. Each with its own known-at time-stamp and possibly with a different valid interval.
 * The identifier is the connection between the different versions.
 * In that sense it is not unique but it does uniquely identify a set of instances of the same entity.   
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
    def existsAt(asOf: DateTime): Boolean = knownAt.isBefore(asOf) || knownAt.isEqual(asOf)
    
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
}

