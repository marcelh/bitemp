package bitemporal.repository

import org.joda.time.DateTime
import org.joda.time.DateTime.now
import org.joda.time.Interval
import org.scalatest.FunSpec

import bitemporal.BitemporalEntity
import bitemporal.BitemporalRepository

/**
 * Define behavior for BitemporalRepository implementations.
 */
trait BitemporalRepositoryBehavior  { this: FunSpec =>

    // time stamps for valid-at / valid interval
	val t0 = new DateTime(2000,1,1,0,0)
	val t1 = new DateTime(2001,1,1,0,0)
	val t2 = new DateTime(2002,1,1,0,0)
	val t3 = new DateTime(2003,1,1,0,0)
	val t4 = new DateTime(2004,1,1,0,0)
	val t5 = new DateTime(2005,1,1,0,0)
	val t6 = new DateTime(2006,1,1,0,0)
	val t7 = new DateTime(2007,1,1,0,0)
	val t8 = new DateTime(2008,1,1,0,0)
	val t9 = new DateTime(2009,1,1,0,0)
	val tAll = Seq(t0, t1, t2, t3, t4, t5, t6, t7, t8, t9)

	// time stamps for known-at / as-of
	val ka1 = new DateTime(2012,1,1,0,0)
    val ka2 = new DateTime(2012,2,1,0,0)
    val ka3 = new DateTime(2012,3,1,0,0)
    val ka4 = new DateTime(2012,4,1,0,0)
    val ka5 = new DateTime(2012,5,1,0,0)
	
	val valuesA = Map("attr1" -> "abc")
	val valuesB = Map("attr1" -> "def")
	val valuesC = Map("attr1" -> "ghi")

	/**
	 * Filter to remove implementation dependent values from the actual map returned from the underlying repository.
	 * Can be overridden for implementation specific tests.
	 */
	def filterActual(orig: Map[String, Any]): Map[String, Any] = orig
	
    /**
     * Check that there is no entity at each of the given validity time stamps
     * 
     * @param r the repository
     * @param id the entity id
     * @param asOf the as-of time stamp
     * @param validAt the validity time stamps to check
     */
    def assertNoEntityFor(r: BitemporalRepository, id: String, asOf: DateTime, validAt: DateTime*) {
        validAt foreach (t => assert(r.get(id, asOf, t).isEmpty, s"get($id, $asOf, $t).isEmpty"))
    }
    
    /**
     * Check that the entity contains exactly the given set of values at each of the given validity time stamps
     * 
     * @param r the repository
     * @param id the entity id
     * @param expectedValues the expected set of values
     * @param asOf the as-of time stamp
     * @param validAt the validity time stamps to check
     */
    def assertEqualFor(r: BitemporalRepository, id: String, expectedValues: Map[String, Any], 
            asOf: DateTime, validAt: DateTime*) 
    {
        validAt foreach (t => {
            val actual = r.get(id, t, asOf)
            assert(actual.isDefined, s"get($id, $t, $asOf).isDefined\n")
            val actualValues = filterActual(actual.get.values)
            assert(actualValues === expectedValues, "expected " + expectedValues + "; got " + actualValues + ". \n")
        })
    }
    
    def assertEqual(index: Int, e1: BitemporalEntity, e2: BitemporalEntity) {
        assert(e1.id === e2.id, s"id not equal at index $index")
        assert(e1.validInterval === e2.validInterval, s"validInterval not equal at index $index")
        assert(e1.knownAt === e2.knownAt, s"trxTimestamp not equal at index $index")
        assert(filterActual(e1.values) === filterActual(e2.values), s"values not equal at index $index")
    }

    def assertEqual(s1: Seq[BitemporalEntity], s2: Seq[BitemporalEntity]) {
        s1.zip(s2).zipWithIndex.foreach{ case ((e1, e2), index) => assertEqual(index, e1, e2) }
        assert(s1.size === s2.size, s"s1=$s1 s2=$s2; ")
    }
    
    /**
     * Test valid time behavior of BitemporalRepository
     */
    def validTimeInBitemporalRepository(repository: => BitemporalRepository) {

        it("should return None on all timestamps if we didn't put anything in it") {
            val r = repository
            assertNoEntityFor(r, "a", now, tAll:_*)
        }
        
        it("should return either None or the value we just put in depending on the validAt timestamp") {
            val r = repository
            r.put("e1", valuesA, new Interval(t1, t3))

            assertNoEntityFor(r, "e1", now, t0,t3,t4)
            assertEqualFor(r, "e1", valuesA, now, t1, t2)
        }

        it("should return the latest inserted value") {
            val r = repository
    		r.put("e1", valuesB, new Interval(t1, t4))
            r.put("e1", valuesC, new Interval(t3, t6))

            assertNoEntityFor(r, "e1", now, t0, t6)
            assertEqualFor(r, "e1", valuesB, now, t1, t2)
            assertEqualFor(r, "e1", valuesC, now, t3, t4, t5)
        }

        it("should return the correct value when multiple overlapping intervals are present") {
        	val r = repository
    		r.put("e1", valuesA, new Interval(t0, t6))
            r.put("e1", valuesB, new Interval(t2, t9))
            r.put("e1", valuesC, new Interval(t4, t6))

            assertNoEntityFor(r, "e1", now, t9)
            assertEqualFor(r, "e1", valuesA, now, t0, t1)
            assertEqualFor(r, "e1", valuesB, now, t2, t3, t6, t7, t8)
            assertEqualFor(r, "e1", valuesC, now, t4, t5)
        }
    }
    
    /**
     * Test as-of time behavior
     */
    def asOfInBitemporalRepository(repository: => BitemporalRepository) {
        
        it("should return either None or the value we just put in depending on the asOf timestamp") {
            val r = repository
            val e1 = r.put("e1", valuesA, new Interval(t1, t3), ka1)
            val e2 = r.put("e1", valuesB, new Interval(t1, t3), ka2)
            
            assertNoEntityFor(r, "e1", e2.knownAt.minusMillis(1), t2)
            assertEqualFor(r, "e1", valuesA, e1.knownAt, t2)
            assertEqualFor(r, "e1", valuesA, e1.knownAt.plusMillis(1), t2)
            assertEqualFor(r, "e1", valuesB, e2.knownAt, t2)
            assertEqualFor(r, "e1", valuesB, e2.knownAt.plusMillis(1), t2)
        }
    }
    
    def asOfIntervalInBitemporalRepository(repository: => BitemporalRepository) {
        def setup = {
	    	val r = repository
			val e1 = r.put("e1", valuesA, new Interval(t1, t3), ka1)
			val e2 = r.put("e1", valuesB, new Interval(t1, t3), ka2)
			val e3 = r.put("e1", valuesC, new Interval(t1, t3), ka3)
			(r, e1, e2, e3)
        }
        
        it("should return an empty sequence if the as-of range is before the first entity") {
            val (r, e1, e2, e3) = setup
            val result = r.get("e1", new Interval(e1.knownAt.minusMillis(10), e1.knownAt.minusMillis(5)))
            assert(result.isEmpty)
        }
        it("should return an empty sequence if the as-of range is after the last entity") {
            val (r, e1, e2, e3) = setup
            val result = r.get("e1", new Interval(e3.knownAt.plusMillis(5), e3.knownAt.plusMillis(10)))
            assert(result.isEmpty)
        }
        it("should return a sequence with only e1 if the as-of range only includes the e1 entity") {
            val (r, e1, e2, e3) = setup
            val result = r.get("e1", new Interval(e1.knownAt.minusMillis(5), e1.knownAt.plusMillis(1)))
            assertEqual(result, Seq(e1))
        }
        it("should return a sequence with e1 and e2 if the as-of range includes the e1 and e2 entities") {
            val (r, e1, e2, e3) = setup
            val result = r.get("e1", new Interval(e1.knownAt.minusMillis(5), e2.knownAt.plusMillis(1)))
            assertEqual(result, Seq(e2, e1)) // note the ordering is relevant!
        }
        it("should return a sequence with e1, e2 and e3 if the as-of range includes the e1, e2 and e3 entities") {
            val (r, e1, e2, e3) = setup
            val result = r.get("e1", new Interval(e1.knownAt.minusMillis(5), e3.knownAt.plusMillis(5)))
            assertEqual(result, Seq(e3, e2, e1)) // note the ordering is relevant!
        }
        it("should return a sequence with e2 and e3 if the as-of range includes the e2 and e3 entities") {
            val (r, e1, e2, e3) = setup
            val result = r.get("e1", new Interval(e2.knownAt.minusMillis(1), e3.knownAt.plusMillis(1)))
            assertEqual(result, Seq(e3, e2)) // note the ordering is relevant!
        }
    }
}
