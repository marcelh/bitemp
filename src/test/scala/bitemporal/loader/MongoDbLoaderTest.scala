package bitemporal.loader

import java.io.File
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.FunSpec
import com.mongodb.casbah.MongoConnection

class MongoDbLoaderTest extends FunSpec with ShouldMatchers {

    describe("A MongoDbLoader") {
        it("should not load the same file twice") {
            val m = MongoConnection()("testdb")
            m.dropDatabase()
            val loader = new MongoDbLoader(m)
            loader.load(dummyParser)
            assert(m("meta").size === 1)
            assert(m("data").size === 2)
            loader.load(dummyParser)
            assert(m("meta").size === 1)
            assert(m("data").size === 2)
        }
        it("should load data from tsv files") {
            val m = MongoConnection()("testdb")
            m.dropDatabase()
            val loader = new MongoDbLoader(m)
            loader.load(new CsvBatchParser(new File("src/test/resources/test1.tsv"), '\t'))
        }
    }
    
    def dummyParser: BatchParser = new BatchParser {
        val name = "dummy"
        def metaData: Map[String, String] = Map("name"->"dummy")
		def identifier: String = "abcdefg"
		def processData(f: Map[String, Any] => Unit) {
        	f(Map("a" -> 1, "b" -> "A"))
            f(Map("a" -> 2, "b" -> "B"))
        } 
    }
}
