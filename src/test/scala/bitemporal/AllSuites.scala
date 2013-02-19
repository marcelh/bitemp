package bitemporal

import org.scalatest.Suites
import bitemporal.store.BitemporalInMemStoreTest
import bitemporal.store.BitemporalMongoDbStoreTest
import org.scalatest.Suites

class AllSuites extends Suites(
    new BitemporalEntityTest,
    new BitemporalInMemStoreTest,
    new BitemporalMongoDbStoreTest
)