package bitemporal.loader
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.path.FunSpec
import java.io.File
import scala.collection.mutable.LinkedList
import scala.collection.mutable.ListBuffer

class CsvBatchParserTest extends FunSpec with ShouldMatchers {

    describe("CsvBatchParser") {
        
        it("should parse an empty file") {
            val filename = "/dev/null"
        	val p = new CsvBatchParser(new File(filename), ' ')
        	assert(p.identifier != null, "identifier is null")
        	assert(!p.identifier.isEmpty, "identifier is empty string")
        	var count = 0
        	p.processData(_ => count = count + 1)
        	assert(count === 0)
        	assert(p.metaData("file-name") === filename)
        	assert(!p.metaData("batch-id").isEmpty())
        }
        it("should parse a file with only a header") {
            val filename = "src/test/resources/header-only.csv"
            val p = new CsvBatchParser(new File(filename), ' ')
            var count = 0
        	p.processData(_ => count = count + 1)
        	assert(count === 0)
        	assert(p.metaData("file-name") === filename)
        	assert(!p.metaData("batch-id").isEmpty())
        }
        it("should parse a file with two records") {
            val filename = "src/test/resources/two-records.csv"
            val p = new CsvBatchParser(new File(filename), ' ')
            var records = ListBuffer[Map[String,Any]]()
        	p.processData(r => records += r)
        	assert(records.size === 2)
        	assert(p.metaData("file-name") === filename)
        	assert(!p.metaData("batch-id").isEmpty())
        	assert(records(0) === Map("aap"->"aa","noot"->"nn","mies"->"mm"))
        	assert(records(1) === Map("aap"->"bb","noot"->"cc","mies"->"dd"))
        }
    }
}