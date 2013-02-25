package bitemporal.store;

import org.scalatest.FunSpec

class BitemporalInMemStoreTest extends FunSpec with BitemporalStoreBehavior {

    def emptyStore = new BitemporalInMemRepository
    
    describe("A BitemporalInMemoryStore") {
        it should behave like validTimeInBitemporalStore(emptyStore)
    }
}
