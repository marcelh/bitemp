package bitemporal.loader

case class LoaderEntity(id: String, data: Map[String, String])

/**
 * Repository to store loaded batches.
 */
trait LoaderRepository {

    /**
     * Store loader entity and returns Right if this is a new entity.
     * Does not store this new entity if there is alread an entity with the given id and returns the existing entiry
     * as Left. 
     */
    def putOrGet(id: String, meta: Map[String, String]): Either[LoaderEntity, LoaderEntity]
    
    /**
     * Returns the entity if it exists or None if it does not.
     */
    def get(id: String): Option[LoaderEntity]
}