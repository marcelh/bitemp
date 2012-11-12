package bitemporal;

import bitemporal._
import bitemporal.store.BitemporalInMemStore
import org.joda.time.{DateTime,Interval}
import org.scalatest.FunSpec

class BitemporalInMemStoreTest extends FunSpec with BitemporalStoreBehavior {

    def emptyStore = { println("new store"); new BitemporalInMemStore[Value] }
    
    describe("A BitemporalInMemoryStore") {

        it should behave like bitemporalStore(emptyStore)
        
    }
}
