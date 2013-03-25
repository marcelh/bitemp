package bitemporal

import org.joda.time.DateTime
import org.joda.time.DateTimeZone.UTC
import org.joda.time.Interval

/** Companion object for the BitemporalRepository trait. */
object BitemporalRepository {
    /** The smallest possible time stamp that can be stored */ 
    val startOfTime = new DateTime(0, 1, 1, 0, 0, UTC)
    /** The largest possible time stamp that can be stored */
    val endOfTime = new DateTime(9999, 12, 31, 23, 59, UTC)
}

/**
 * An interface to store bi-temporal entities
 */
trait BitemporalRepository {

    /**
     * Get the entity valid on and known at the specified time. 
     * <p>
     * More formally: find the entity which matches the given id, has a validInterval that contains the given validAt 
     * time-stamp, has a trxTimestamp time-stamp that is the same or is before the given asOf time stamp AND there 
     * exists no other entity which matches the same conditions but has an asOf between the first found entity 
     * and the given trxTimestamp time-stamp.
     * <p>
     * Note that the returned entity (if any) contains the original valid interval which might not be correct anymore
     * on the given asOf date-time. This might change in the future...
     * 
     * @param id the entity id (should not be null or empty)
     * @param validAt at which time the entity should be valid
     * @param asOf at which time the entity should have been in the store
     * @return Some[Entity] or None when no such entity exists in the store
     */
    def get(id: String, validAt: DateTime, asOf: DateTime): Option[BitemporalEntity]
    
    /**
     * Get the list of entities with the given id and where the transaction time-stamp falls inside the given interval.
     * 
     * @param id the entity id (should not be null or empty)
     * @param asOfInterval the interval in which the as-of time stamp should fall
     * @return a (possibly empty) sequence of entities ordered from highest (newest) transaction time stamp to lowest 
     * (oldest).
     */
    def get(id: String, asOfInterval: Interval): Seq[BitemporalEntity]
    
    /**
     * Add an entity with the given value to the store.
     * <p>
     * Although we don't make any assumption about the underlying data store, the most viable way to support the above
     * functionality is to simply store a new entity version with each call to this method.  
     * 
     * @param id the entity id (should not be null or empty)
     * @param values the attribute values for the entity
     * @return the created entity
     */
    def put(id: String,
            values: Map[String, Any], 
            validInterval: Interval): BitemporalEntity
}
