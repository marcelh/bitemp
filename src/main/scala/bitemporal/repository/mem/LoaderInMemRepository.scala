package bitemporal.repository.mem

import scala.collection.mutable
import bitemporal.loader.LoaderEntity
import bitemporal.loader.LoaderRepository

/**
 * Loader implementation that stores all entities in an in-memory data structure.
 */
class LoaderInMemRepository extends LoaderRepository {

    val entities = mutable.Map[String, LoaderEntity]()
    
    def putOrGet(key: String, meta: Map[String, String]): Either[LoaderEntity, LoaderEntity] = {
        val oldEnt = entities.get(key)
        if (oldEnt.isDefined)
            Left(oldEnt.get)
        else {
        	val newEnt = LoaderEntity(key, meta)
        	entities.put(key, newEnt)
        	Right(newEnt)
        }
    }
    
    def get(id: String): Option[LoaderEntity] = entities.get(id)
}