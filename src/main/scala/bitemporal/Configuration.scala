package bitemporal

import scala.collection.mutable

private object ConfigurationStore {
    val entries = mutable.Map[String, AnyRef]()

    def put(key: String, value: AnyRef) {
        entries += ((key, value))
    }

    def get[A] = entries.values.find(_.isInstanceOf[A]).asInstanceOf[Option[A]]
}

trait Configured {

    def configured[A]: A = ConfigurationStore.get[A].get
}

trait Configuration {

    def configure[R <: AnyRef](f: => R) = {
        val a = f
        ConfigurationStore.put(a.getClass.getName, a)
        a
    }
}