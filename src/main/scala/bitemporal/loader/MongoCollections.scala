package bitemporal.loader

import bitemporal.Configured
import com.mongodb.casbah.MongoCollection
import com.mongodb.casbah.MongoDB

trait MongoCollections extends Configured {
    
    def mongoDB: MongoDB = configured[MongoDB]
	def metaCollection: MongoCollection = (configured[MongoDB])("meta")
    def dataCollection: MongoCollection = (configured[MongoDB])("data")
}