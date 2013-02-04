
/**
 * <h2>Bi-temporal data store</h2>
 * 
 * A bi-temporal data store stores data with two time dimensions.
 * 
 * One is te <i>know at</i> time. 
 * Which is here called the transaction time.
 * It is the time when a fact (data) is current/present in the database as stored data.
 *
 * The other is the <i>valid at</i> time interval. 
 * This specifies at what time interval this data is valid.
 * In other words: the time a fact was/is/will be true in the modeled reality.
 * 
 * There are two ways of time-stamping:
 *   - tuple/entity time-stamping
 *   - attribute time-stamping
 *   
 * Here we use tuple/entity time-stamping because:
 *   - having two time dimension on each attribute is more overhead than per entity,
 *   - it seems more logical (and understandable) to reason about bi-temporality (?) at the entity level than on the 
 *     attribute level.
 *   
 * An entity is a container for attributes, as such an entity with its attribute (values) is a logical unit and
 * should be stored (modified/updated/deleted) as a whole. 
 */
package object bitemporal {}
