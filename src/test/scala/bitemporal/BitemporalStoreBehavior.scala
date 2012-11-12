package bitemporal;

import bitemporal._
import bitemporal.store.BitemporalInMemStore
import org.joda.time.{DateTime,Interval}
import org.scalatest.FunSpec

trait BitemporalStoreBehavior  { this: FunSpec =>

    type Value = Int
    type MapEntity = Entity[Value]
    
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
    
    def assertEmptyFor(s: BitemporalStore[Value], id: String, validAt: DateTime*) {
        validAt foreach (t => assert(s.get(id, t).isEmpty, "get(" + id + "," + t + ").isEmpty"))
    }
    
    def assertEqualFor(s: BitemporalStore[Value], id: String, expected: Value, validAt: DateTime*) {
        validAt foreach (t => {
            val actual = s.get(id, t)
            assert(actual.isDefined, 
                    "get(" + id + "," + t + ").isDefined\n" + s.dump)
            assert(actual.get.value === expected, 
                    "expected " + expected + "; got " + actual.get.value + "\n" + s.dump)
        })
    }
    
    def bitemporalStore(store: => BitemporalStore[Value]) {

        it("should return None on all timestamps if we didn't put anything in it") {
            val s = store
            assertEmptyFor(s, "a", t0,t1,t2,t3,t4,t5,t6,t7,t8,t9)
        }
        
        it("should return either None or the value we just put in depending on the validAt timestamp") {
            val s = store
            s.put("a", 1, new Interval(t1, t3))

            assertEmptyFor(s, "a", t0, t3)
            assertEqualFor(s, "a", 1, t1, t2)
        }

        it("should return the latest inserted value") {
            val s = store
    		s.put("a", 1, new Interval(t1, t4))
            s.put("a", 2, new Interval(t3, t6))

            assertEmptyFor(s, "a", t0, t6)
            assertEqualFor(s, "a", 1, t1, t2)
            assertEqualFor(s, "a", 2, t3, t4, t5)
        }
    }
}
