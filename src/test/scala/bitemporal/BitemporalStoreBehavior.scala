package bitemporal;

import bitemporal._
import bitemporal.store.BitemporalInMemStore
import org.joda.time.{DateTime,Interval}
import org.scalatest.FunSpec

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

    val sa1 = StringAttribute("StrAttr1")
    val ia1 = IntAttribute("IntAttr1")

    /**
     * Check that there is no entity at each of the given validity time stamps
     * 
     * @param s the store
     * @param id the entity id
     * @param validAt the validity time stamps to check
     */
    def assertNoEntityFor(s: BitemporalStore, id: String, validAt: DateTime*) {
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
    def assertEqualFor(s: BitemporalStore, id: String, expectedValues: Set[AttributeValue], validAt: DateTime*) {
        validAt foreach (t => {
            val actual = s.get(id, t)
            assert(actual.isDefined, "get(" + id + "," + t + ").isDefined\n")
            val actualValues = actual.get.values.values.toSet
            assert(actualValues === expectedValues, "expected " + expectedValues + "; got " + actual.get.values + "\n")
        })
    }
    
    /**
     * Test valid time behavior of BitemporalStore
     */
    def validTimeInBitemporalStore(store: => BitemporalStore) {
        val attr1 = StringAttribute("attr1")
        val attr2 = IntAttribute("attr2")
        val v1a = attr1.createValue("abc")
        val v1b = attr1.createValue("xyz")
        val v2a = attr2.createValue(123)
        val v2b = attr2.createValue(321)

        it("should return None on all timestamps if we didn't put anything in it") {
            val s = store
            assertNoEntityFor(s, "a", t0,t1,t2,t3,t4,t5,t6,t7,t8,t9)
        }
        
        it("should return either None or the value we just put in depending on the validAt timestamp") {
            val s = store
            s.put("e1", Set(v1a), new Interval(t1, t3))

            assertNoEntityFor(s, "e1", t0,t3,t4)
            assertEqualFor(s, "e1", Set(v1a), t1, t2)
        }

        it("should return the latest inserted value") {
            val s = store
    		s.put("e1", Set(v1a,v2a), new Interval(t1, t4))
            s.put("e1", Set(v1b,v2b), new Interval(t3, t6))

            assertNoEntityFor(s, "e1", t0, t6)
            assertEqualFor(s, "e1", Set(v1a,v2a), t1, t2)
            assertEqualFor(s, "e1", Set(v1b,v2b), t3, t4, t5)
        }
    }
    
    def mergedValidTimeInBitemporalStore(store: => BitemporalStore) {
        
        it("should return a merged record for two overlapping ranges") {
            val s = store
    		val v1 = sa1.createValue("aa")
    		val v2 = ia1.createValue(1)
    		s.put("e1", Set(v1,v2), new Interval(t1, t4))
            s.put("e1", Set(v1,v2), new Interval(t3, t6))
            val actual = s.get("e1", t3)
            assert(actual.isDefined)
            assert(actual.get.id === "e1")
            assert(actual.get.validInterval === new Interval(t1, t6))
        }
        it("should return a merged record for two overlapping ranges with asOf for first stored entity") {
            val s = store
    		val v1 = sa1.createValue("aa")
    		val v2 = ia1.createValue(1)
    		s.put("e1", Set(v1,v2), new Interval(t1, t4))
            s.put("e1", Set(v1,v2), new Interval(t3, t6))
            val actual = s.get("e1", t2)
            assert(actual.isDefined)
            assert(actual.get.id === "e1")
            assert(actual.get.validInterval === new Interval(t1, t6))
        }
        it("should return a merged record for three overlapping ranges") {
            val s = store
    		val v1 = sa1.createValue("aa")
    		val v2 = ia1.createValue(1)
    		s.put("e1", Set(v1,v2), new Interval(t1, t3))
            s.put("e1", Set(v1,v2), new Interval(t4, t6))
            s.put("e1", Set(v1,v2), new Interval(t2, t5))
            val actual = s.get("e1", t3)
            assert(actual.isDefined)
            assert(actual.get.id === "e1")
            assert(actual.get.validInterval === new Interval(t1, t6))
        }
    }
}
