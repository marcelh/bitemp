package bitemporal.store

import com.typesafe.config.ConfigFactory
import com.mongodb.casbah.MongoConnection
import com.mongodb.casbah.MongoDB
import com.typesafe.config.Config
import com.mongodb.casbah.commons.MongoDBObject

/**
 * Mongo DB connection control trait
 */
trait MongoControl {

    /**
     * To be injected configuration object
     */
    def config: Config
    
    private def mongoConnection: MongoConnection = {
        val host = config.getString("mongo.host")
        val port = config.getInt("mongo.port")
        MongoConnection(host, port)
    }
    
    private def mongoDB(conn: MongoConnection): MongoDB = {
        val database = config.getString("mongo.database")
    	conn(database)
    }

    /**
     * Apply function to an explicitly given Mongo DB connection.
     * 
     * @param conn the connection object as returned from 'mongoConnection'.
     * @param f the function to apply.
     * @return the result of function application.
     */
    def using[T](conn: MongoConnection)(f: MongoConnection => T): T = {
        // TODO use connection pool
        try {
            f(conn)
        }
        finally {
            conn.close()
        }
    }
    
    /**
     * Apply function to a Mongo DB connection.
     * 
     * @param f the function to apply.
     * @return the result of the function application.
     */
    def usingMongo[T](f: MongoConnection => T): T = using(mongoConnection)(f)
    
    /**
     * Returns the 'attributes' collection.
     */
    def attrsCollection(conn: MongoConnection) = mongoDB(conn)(config.getString("mongo.collections.attrs"))
    /**
     * Return the 'data' collection.
     */
    def dataCollection(conn: MongoConnection) = mongoDB(conn)(config.getString("mongo.collections.data"))
    
    /**
     * Creates necessary indexes (if not already done).
     */
    def ensureIndexes(conn: MongoConnection) {
        attrsCollection(conn).ensureIndex(MongoDBObject(("id" -> 1), ("tx" -> 1), ("valid-from" -> 1)))
    }
}