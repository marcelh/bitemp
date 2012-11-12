package bitemporal

import org.joda.time.{DateTime,Interval}
import org.joda.time.DateTimeZone.UTC

/*
 * Bi-temporal data store
 * 
 * valid time := The time a fact was/is/will be true in the modeled reality
 * 
 * transaction time := The time when a fact is current/present in the database as stored data
 * 
 * Two ways of time-stamping:
 *   - tuple/entity time-stamping
 *   - attribute time-stamping
 *   
 * Here we use tuple/entity time-stamping because:
 *   - I think bitemporal attributes it too much overhead,
 *   - it seems more logical to reason about bitemporality (?) at the entity level than on the attribute level.
 *   
 * An entity is a container for attributes, as such an entity with its attribute (values) is a logical unit and
 * should be stored (modified/updated/deleted) as a whole. 
 */

/**
 * Bi-temporal trait
 */
trait Bitemporal[E] {
    def trxTimestamp: DateTime
    def validInterval: Interval
}

/**
 * The entity class with a generic type for it's data (attribute value(s)).
 */
case class Entity[V](
        id: String,
		value: V,
		validInterval: Interval,
		trxTimestamp: DateTime
	) extends Bitemporal[V]


/**
 * An interface to store bi-temporal entities
 */
trait BitemporalStore[V] {

    val startOfTime = new DateTime(0, 1, 1, 0, 0, UTC)
    val endOfTime = new DateTime(9999, 12, 31, 23, 59, UTC)

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
            asOf: DateTime = DateTime.now): Option[Entity[V]]
    
    /**
     * Modify and/or add an entity with the given value to the store.
     * <p>
     * Depending on the implementation entities could be split, merged or created.
     * The returned entity might have a validInterval that is larger than specified.
     * 
     * @param id the entity id (should not be null or empty)
     * @param value the data for the entity
     * @return the entity
     */
    def put(id: String,
            value: V, 
            validInterval: Interval): Entity[V]
    
    def dump: String
}

