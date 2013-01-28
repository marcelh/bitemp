package bitemporal.store;

import java.io.File
import java.util.concurrent.TimeUnit.SECONDS
import org.joda.time.Interval
import org.scalatest.BeforeAndAfterAll
import org.scalatest.FunSpec
import com.typesafe.config.ConfigFactory
import com.yammer.metrics.reporting.CsvReporter
import bitemporal.BitemporalStore.endOfTime
import bitemporal.BitemporalStore.startOfTime
import scalax.file.Path

class BitemporalMongoDbStoreTest extends FunSpec with BitemporalStoreBehavior with BeforeAndAfterAll with MongoControl {

    val config = ConfigFactory.load()
    
    val timingsDir = new File("target/timings/" + getClass.getSimpleName)
    
    override def beforeAll(configMap: Map[String, Any]) {
        Path(timingsDir).deleteRecursively(true, false)
    	timingsDir.mkdirs()
    	CsvReporter.enable(timingsDir, 1, SECONDS);
        println(configMap)
    }
    
    def emptyStore = {
        usingMongo { conn =>
            attrsCollection(conn).drop
            dataCollection(conn).drop
        }
        new BitemporalMongoDbStore(config)
    }
    
    describe("A BitemporalMongoDbStore") {

        //it should behave like validTimeInBitemporalStore(emptyStore)
        
        //it should behave like mergedValidTimeInBitemporalStore(emptyStore)
        
        it("should be able to store something") {
            val s = emptyStore
            try {
            1 to 10000 foreach(i => 
            	s.put("someid"+i, Map("aap" -> "noot"), new Interval(startOfTime, endOfTime))
        	)
            } catch {
                case t:Throwable => 
                	println("Caught: " + t.getMessage())
                    Thread.sleep(60*1000)
            }
        }
    }
}
