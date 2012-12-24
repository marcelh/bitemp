package bitemporal;

import bitemporal._
import bitemporal.store.BitemporalInMemStore
import org.joda.time.{DateTime,Interval}
import org.scalatest.FunSpec
import bitemporal.loader.LineParser
import scala.io.Source
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import com.mongodb.casbah.MongoConnection
import bitemporal.loader.MongoDbLoader
import bitemporal.loader.Parser
import bitemporal.loader.TsvParser
import java.io.File

class MongoDbLoaderTest extends FunSpec {

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
            loader.load(new TsvParser(new File("src/test/resources/test1.tsv")))
        }
    }
    
    def dummyParser: Parser = new Parser {
        val iter = List(
                Map("a" -> 1, "b" -> "A"),
                Map("a" -> 2, "b" -> "B")).iterator
        def id: String = "test1"
	    def name: String = "Test 1"
	    def metaData: Map[String, Any] = Map("sha1" -> 123)
	    def hasNext: Boolean = iter.hasNext
	    def next: Map[String, Any] = iter.next()
	    def close {}
    }
}
