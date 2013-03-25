package bitemporal.repository.mongo

import java.io.File
import java.util.concurrent.TimeUnit.SECONDS

import org.joda.time.DateTime
import org.joda.time.Interval
import org.scalatest.BeforeAndAfterAll
import org.scalatest.FunSpec

import com.typesafe.config.ConfigFactory
import com.yammer.metrics.reporting.CsvReporter

import bitemporal.BitemporalRepository.endOfTime
import bitemporal.BitemporalRepository.startOfTime
import bitemporal.repository.BitemporalRepositoryBehavior
import scalax.file.Path

class BitemporalMongoDbRepositoryTest extends FunSpec 
		with BitemporalRepositoryBehavior 
		with BeforeAndAfterAll 
		with MongoControl
{
    val config = ConfigFactory.load()
    val timingsDir = new File("target/timings/" + getClass.getSimpleName)
    
    /* filter out MongoDB specific values so we can simply compare the 'values' map */
    override def filterActual(orig: Map[String, Any]): Map[String, Any] = {
        val specialKeys = Set("_id", "id", "valid-from", "valid-until", "tx")
		orig filter { case (k, v) => !specialKeys.contains(k) }
    }
    
    override def beforeAll(configMap: Map[String, Any]) {
        Path(timingsDir).deleteRecursively(true, false)
    	timingsDir.mkdirs()
    	CsvReporter.enable(timingsDir, 1, SECONDS);
    }
    
    def emptyRepository = {
        usingMongo { conn =>
            bitempCollection(conn).drop
            ensureIndexes(conn)
        }
        new BitemporalMongoDbRepository(config)
    }
    
    describe("A BitemporalMongoDbRepository") {
    	
    	it("should be able to store something") {
    		val s = emptyRepository
			1 to 100 foreach(i => s.put("someid"+i, Map("aap" -> "noot"), new Interval(startOfTime, endOfTime)))
    		1 to 100 foreach(i => assert(s.get("someid"+i, DateTime.now, DateTime.now).isDefined))
    	}

        it should behave like validTimeInBitemporalRepository(emptyRepository)
        it should behave like asOfInBitemporalRepository(emptyRepository)
        it should behave like asOfIntervalInBitemporalRepository(emptyRepository)
    }
}
