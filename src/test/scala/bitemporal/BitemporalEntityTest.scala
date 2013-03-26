package bitemporal

import org.scalatest.FunSpec
import org.joda.time.Interval
import org.joda.time.DateTime

class BitemporalEntityTest extends FunSpec with BitemporalEntityBehavior {

    val anEntity = new BitemporalEntity {
        val id = "myid"
        val values = Map("a"->1, "b"->2)
        val validInterval= new Interval(new DateTime(2000,1,1,0,0), new DateTime(2010,1,1,0,0))
        val knownAt = new DateTime(2000,1,1,0,0)
    }
    
    describe("A BitemporalEntity") {
        it should behave like existsAtBehavior(anEntity)
        it should behave like isValidAtBehavior(anEntity)
        it should behave like matchesBehavior(anEntity)
    }
}

trait BitemporalEntityBehavior { this: FunSpec =>
    
    def inTheMiddle(iv: Interval): DateTime = new DateTime((iv.getStartMillis() + iv.getEndMillis()) / 2) 
    
    def existsAtBehavior(e: => BitemporalEntity) {
        it("should not exist when asOf is before trxTimestamp") {
        	assert( !e.existsAt(e.knownAt.minusMillis(1)) )
        }
        it("should exist when asOf is equal to trxTimestamp") {
        	assert( e.existsAt(e.knownAt) )
        }
        it("should exist when asOf is after trxTimestamp") {
        	assert( e.existsAt(e.knownAt.plusMillis(1)) )
        }
    }
    
    def isValidAtBehavior(e: => BitemporalEntity) {
    	it("should not be valid when t is before the validInterval") {
    		assert( !e.isValidAt(e.validInterval.getStart().minusMillis(1)) )
    	}
    	it("should be valid when t is at the start of validInterval") {
    		assert( e.isValidAt(e.validInterval.getStart()) )
    	}
    	it("should be valid when t is inside the validInterval") {
    		assert( e.isValidAt(inTheMiddle(e.validInterval)) )
    	}
    	it("should not be valid when t is at the end of the validInterval") {
    		assert( !e.isValidAt(e.validInterval.getEnd()) )
    	}
    	it("should not be valid when t is after the end of the validInterval") {
    		assert( !e.isValidAt(e.validInterval.getEnd().plusMillis(1)) )
    	}
    }
    
    def matchesBehavior(e: => BitemporalEntity) {

        // id
        it("should not match when the id is different") {
            assert(!e.matches(e.id + "x", inTheMiddle(e.validInterval), e.knownAt.plusDays(1)))
        }

        // validAt
        it("should not match when validAt is just before the start of the validInterval") {
            assert(!e.matches(e.id, new DateTime(e.validInterval.getStartMillis() - 1), e.knownAt.plusDays(1)))
        }
        it("should match when validAt is at the start of the validInterval") {
            assert(e.matches(e.id, e.validInterval.getStart(), e.knownAt.plusDays(1)))
        }
        it("should match when validAt is in the middle of the validInterval") {
        	assert(e.matches(e.id, inTheMiddle(e.validInterval), e.knownAt.plusDays(1)))
        }
        it("should match when validAt is just before the end of the validInterval") {
            assert(e.matches(e.id, new DateTime(e.validInterval.getEndMillis() - 1), e.knownAt.plusDays(1)))
        }
        it("should not match when validAt is at the end of the validInterval") {
            assert(!e.matches(e.id, e.validInterval.getEnd(), e.knownAt.plusDays(1)))
        }
        it("should not match when validAt is after the end of the validInterval") {
            assert(!e.matches(e.id, new DateTime(e.validInterval.getEndMillis()+1), e.knownAt.plusDays(1)))
        }
        
        // asOf
        it("should not match when asOf is just before the trxTimestamp") {
            assert(!e.matches(e.id, inTheMiddle(e.validInterval), e.knownAt.minusMillis(1)))
        }
        it("should match when the asOf is the same as the trxTimestamp") {
            assert(!e.matches(e.id + "x", inTheMiddle(e.validInterval), e.knownAt))
        }
    }
}