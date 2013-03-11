package bitemporal

import org.scalatest.Suites

import bitemporal.repository.BitemporalInMemRepositoryTest
import bitemporal.repository.BitemporalMongoDbRepositoryTest

class AllSuites extends Suites(
    new BitemporalEntityTest,
    new BitemporalInMemRepositoryTest,
    new BitemporalMongoDbRepositoryTest
)