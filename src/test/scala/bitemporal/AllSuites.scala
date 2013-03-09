package bitemporal

import org.scalatest.Suites

import bitemporal.repository.BitemporalInMemStoreTest
import bitemporal.repository.BitemporalMongoDbStoreTest

class AllSuites extends Suites(
    new BitemporalEntityTest,
    new BitemporalInMemStoreTest,
    new BitemporalMongoDbStoreTest
)