package bitemporal.repository;

import org.scalatest.FunSpec

import bitemporal.repository.BitemporalInMemRepository;

class BitemporalInMemStoreTest extends FunSpec with BitemporalStoreBehavior {

    def emptyStore = new BitemporalInMemRepository
    
    describe("A BitemporalInMemoryStore") {
        it should behave like validTimeInBitemporalStore(emptyStore)
    }
}
