package bitemporal;

import bitemporal._
import bitemporal.store.BitemporalInMemStore
import org.joda.time.{DateTime,Interval}
import org.scalatest.FunSpec
import bitemporal.loader.LineParser
import scala.io.Source
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

class LineParserTest extends FunSpec {

    describe("A LineParser") {
        it("should properly behave on empty input") {
            val p = new LineParser(Source.fromString(""))
            assert(p.hasNext === false)
            assert(p.peek === None)
            intercept[IllegalStateException] { p.next }
            intercept[IllegalStateException] { p.expect("bla") }
        }
        it("should return the contents even if it has no newline") {
            val p = new LineParser(Source.fromString("aap"))
            assert(p.hasNext === true)
            assert(p.peek === Some("aap"))
            assert(p.next === "aap")
            assert(p.hasNext === false)
        }
        it("should return the contents without newline") {
            val p = new LineParser(Source.fromString("aap\n"))
            assert(p.hasNext === true)
            assert(p.peek === Some("aap"))
            assert(p.next === "aap")
            assert(p.hasNext === false)
        }
        it("should return two lines") {
            val p = new LineParser(Source.fromString("aap\nnoot"))
            assert(p.hasNext === true)
            assert(p.peek === Some("aap"))
            assert(p.next === "aap")
            assert(p.hasNext === true)
            assert(p.peek === Some("noot"))
            assert(p.next === "noot")
            assert(p.hasNext === false)
        }
    }
}
