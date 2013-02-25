package bitemporal.loader

import scala.io.Source

import org.joda.time.Interval
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.path.FunSpec

import bitemporal.BitemporalRepository.endOfTime
import bitemporal.BitemporalRepository.startOfTime

class CsvBatchTest extends FunSpec with ShouldMatchers {

    describe("CsvBatch") {

        it("should parse an empty file") {
            val source = Source.fromString("")
            val b = new CsvBatch(source, "x", "empty", ' ')
            val iter = b.recordIterator
            assert(iter.hasNext === false)
        }
        it("should parse a file with only a header") {
            val source = Source.fromString("a b\n")
            val b = new CsvBatch(source, "x", "header-only", ' ')
            val iter = b.recordIterator
            assert(iter.hasNext === false)
        }
        it("should parse a file with two records") {
            val source = Source.fromString("a b\n1 2\n3 4\n")
            val b = new CsvBatch(source, "x", "two-records", ' ')
            val iter = b.recordIterator
            assert(iter.hasNext === true)
            val r1 = iter.next()
            assert(r1.recordId === "two-records[0]")
            assert(r1.validInterval === new Interval(startOfTime, endOfTime))
            assert(r1.values === Map("a" -> "1", "b" -> "2"))
            assert(iter.hasNext === true)
            val r2 = iter.next()
            assert(r2.recordId === "two-records[1]")
            assert(r2.validInterval === new Interval(startOfTime, endOfTime))
            assert(r2.values === Map("a" -> "3", "b" -> "4"))
            assert(iter.hasNext === false)
        }
    }
}