package bitemporal.repository.mem

import org.scalatest.FunSpec
import bitemporal.repository.mem.BitemporalInMemRepository
import bitemporal.repository.BitemporalRepositoryBehavior

class BitemporalInMemRepositoryTest extends FunSpec with BitemporalRepositoryBehavior {

    def emptyRepository = new BitemporalInMemRepository
    
    describe("A BitemporalInMemoryStore") {
        it should behave like validTimeInBitemporalRepository(emptyRepository)
        it should behave like asOfInBitemporalRepository(emptyRepository)
        it should behave like asOfIntervalInBitemporalRepository(emptyRepository)
    }
}
