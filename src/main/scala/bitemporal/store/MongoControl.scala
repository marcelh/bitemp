package bitemporal.store

import com.typesafe.config.ConfigFactory
import com.mongodb.casbah.MongoConnection
import com.mongodb.casbah.MongoDB
import com.typesafe.config.Config

trait MongoControl {

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

    def using[T](conn: MongoConnection)(f: MongoConnection => T): T =
        try {
            f(conn)
        }
        finally {
            conn.close()
        }
    
    def usingMongo[T](f: MongoConnection => T): T = using(mongoConnection)(f)
    
    def attrsCollection(conn: MongoConnection) = mongoDB(conn)(config.getString("mongo.collections.attrs"))
    def dataCollection(conn: MongoConnection) = mongoDB(conn)(config.getString("mongo.collections.data"))
}