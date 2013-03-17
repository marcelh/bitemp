package bitemporal.repository;

import org.scalatest.FunSpec

class BitemporalInMemRepositoryTest extends FunSpec with BitemporalRepositoryBehavior {

    def emptyRepository = new BitemporalInMemRepository
    
    describe("A BitemporalInMemoryStore") {
        it should behave like validTimeInBitemporalRepository(emptyRepository)
        it should behave like asOfInBitemporalRepository(emptyRepository)
        it should behave like asOfIntervalInBitemporalRepository(emptyRepository)
    }
}
