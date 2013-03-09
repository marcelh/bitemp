package bitemporal.repository

import com.typesafe.config.ConfigFactory
import com.mongodb.casbah.MongoConnection
import com.mongodb.casbah.MongoDB
import com.typesafe.config.Config
import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.MongoCollection

/**
 * Trait to control MongDB connections, databases and collections.
 */
trait MongoControl {

    /**
     * Configuration object for used by this trait to connect to a MongoDB instance.
     * Configuration properties used by this trait:
     *  - mongo.host: host name or IP number where MongoDB is running
     *  - mongo.port: port number of the MongoDB instance
     *  - mongo.database: name of the MongoDB database
     *  - mongo.collections.loader: name of the collection with info about loaded files
     *  - mongo.collections.bitemp: name of the collection for the bi-temporal data
     * 
     * @return the configuration object containing MongoDB connection properties and collection names.
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
     * Call function with given MongoConnection, closing the connection afterwards.
     * 
     * @param conn the MongoConnection
     * @param f the function to call
     * @return return value of the function
     */
    def using[T](conn: MongoConnection)(f: MongoConnection => T): T =
        try {
            f(conn)
        }
        finally {
            conn.close()
        }
    
    /**
     * Apply function to a Mongo DB connection.
     * 
     * @param f the function to apply.
     * @return the result of the function application.
     */
    def usingMongo[T](f: MongoConnection => T): T = using(mongoConnection)(f)
    
    /**
     * Returns the loader collection.
     */
    def loaderCollection(conn: MongoConnection): MongoCollection = 
        mongoDB(conn)(config.getString("mongo.collections.loader"))
        
    /**
     * Returns the bi-temporal collection.
     */
    def bitempCollection(conn: MongoConnection): MongoCollection = 
        mongoDB(conn)(config.getString("mongo.collections.bitemp"))
    
    /**
     * Create necessary indexes if they didn't already exist.
     * 
     * @param conn a MongoConnection
     */
    def ensureIndexes(conn: MongoConnection) {
        bitempCollection(conn).ensureIndex(MongoDBObject(("id" -> 1), ("tx" -> 1), ("valid-from" -> 1)))
    }
}
