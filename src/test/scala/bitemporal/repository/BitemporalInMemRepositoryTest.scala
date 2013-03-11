package bitemporal.repository;

import org.scalatest.FunSpec

class BitemporalInMemRepositoryTest extends FunSpec with BitemporalRepositoryBehavior {

    def emptyStore = new BitemporalInMemRepository
    
    describe("A BitemporalInMemoryStore") {
        it should behave like validTimeInBitemporalRepository(emptyStore)
    }
}
