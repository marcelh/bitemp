package bitemporal.store;

import bitemporal._
import org.joda.time.{DateTime,Interval}
import org.scalatest.FunSpec
import bitemporal.BitemporalRepository

/**
 * Define behavior for BitemporalStore implementations.
 */
trait BitemporalStoreBehavior  { this: FunSpec =>

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

	/**
	 * Filter to remove implementation dependent values from the actual map returned from the underlaying store.
	 * Can be overridden for implementation specific tests.
	 */
	def filterActual(orig: Map[String, Any]): Map[String, Any] = orig
	
    /**
     * Check that there is no entity at each of the given validity time stamps
     * 
     * @param s the store
     * @param id the entity id
     * @param validAt the validity time stamps to check
     */
    def assertNoEntityFor(s: BitemporalRepository, id: String, validAt: DateTime*) {
        validAt foreach (t => assert(s.get(id, t).isEmpty, "get(" + id + "," + t + ").isEmpty"))
    }
    
    /**
     * Check that the entity contains exactly the given set of values at each of the given validity time stamps
     * 
     * @param s the store
     * @param id the entity id
     * @param expectedValues the expected set of values
     * @param validAt the validity time stamps to check
     */
    def assertEqualFor(s: BitemporalRepository, id: String, expectedValues: Map[String, Any], validAt: DateTime*) {
        validAt foreach (t => {
            val actual = s.get(id, t)
            assert(actual.isDefined, "get(" + id + "," + t + ").isDefined\n")
            val actualValues = filterActual(actual.get.values)
            assert(actualValues === expectedValues, "expected " + expectedValues + "; got " + actualValues + "\n")
        })
    }
    
    /**
     * Test valid time behavior of BitemporalStore
     */
    def validTimeInBitemporalStore(store: => BitemporalRepository) {

        it("should return None on all timestamps if we didn't put anything in it") {
            val s = store
            assertNoEntityFor(s, "a", t0,t1,t2,t3,t4,t5,t6,t7,t8,t9)
        }
        
        it("should return either None or the value we just put in depending on the validAt timestamp") {
            val s = store
            s.put("e1", Map("attr1" -> "abc"), new Interval(t1, t3))

            assertNoEntityFor(s, "e1", t0,t3,t4)
            assertEqualFor(s, "e1", Map("attr1" -> "abc"), t1, t2)
        }

        it("should return the latest inserted value") {
            val s = store
    		s.put("e1", Map("attr1" -> "abc", "attr2" -> 123), new Interval(t1, t4))
            s.put("e1", Map("attr1" -> "xyz", "attr2" -> 321), new Interval(t3, t6))

            assertNoEntityFor(s, "e1", t0, t6)
            assertEqualFor(s, "e1", Map("attr1" -> "abc", "attr2" -> 123), t1, t2)
            assertEqualFor(s, "e1", Map("attr1" -> "xyz", "attr2" -> 321), t3, t4, t5)
        }

        it("should return the correct value when multiple overlapping intervals are present") {
        	val s = store
    		s.put("e1", Map("attr1" -> 1), new Interval(t0, t6))
            s.put("e1", Map("attr1" -> 2), new Interval(t2, t9))
            s.put("e1", Map("attr1" -> 3), new Interval(t4, t6))

            assertNoEntityFor(s, "e1", t9)
            assertEqualFor(s, "e1", Map("attr1" -> 1), t0, t1)
            assertEqualFor(s, "e1", Map("attr1" -> 2), t2, t3, t6, t7, t8)
            assertEqualFor(s, "e1", Map("attr1" -> 3), t4, t5)
        }
    }
}
