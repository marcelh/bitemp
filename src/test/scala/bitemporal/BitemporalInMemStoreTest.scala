package bitemporal;

import bitemporal._
import bitemporal.store.BitemporalInMemStore
import org.joda.time.{DateTime,Interval}
import org.scalatest.FunSpec

class BitemporalInMemStoreTest extends FunSpec with BitemporalStoreBehavior {

    def emptyStore = new BitemporalInMemStore
    
    describe("A BitemporalInMemoryStore") {

        //it should behave like validTimeInBitemporalStore(emptyStore)
        it should behave like mergedValidTimeInBitemporalStore(emptyStore)
        
    }
}
