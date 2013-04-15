package bitemporal.repository.mongo

import com.mongodb.casbah.MongoCollection
import com.mongodb.casbah.MongoConnection
import com.mongodb.casbah.MongoDB
import com.mongodb.casbah.commons.MongoDBObject
import com.typesafe.config.Config
import com.mongodb.MongoException

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
     * 
     * @return the configuration object containing MongoDB connection properties.
     */
    def config: Config
    
    private def mongoConnection: MongoConnection = {
        val host = config.getString("mongo.host")
        val port = config.getInt("mongo.port")
        MongoConnection(host, port)
    }
    
    def mongoDB(conn: MongoConnection): MongoDB = {
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
    
}
