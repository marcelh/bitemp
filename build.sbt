
name := "bitemp"

version := "0.1"

scalaVersion := "2.9.2"

scalacOptions += "-deprecation"

libraryDependencies += "joda-time" % "joda-time" % "2.1" withSources()

libraryDependencies += "org.joda" % "joda-convert" % "1.2"

//libraryDependencies += "org.specs2" %% "specs2" % "1.12.1" % "test"

libraryDependencies += "org.scalatest" %% "scalatest" % "2.0.M3"

libraryDependencies += "junit" % "junit" % "4.10"

libraryDependencies += "org.mongodb" %% "casbah" % "2.4.1"

libraryDependencies += "com.weiglewilczek.slf4s" % "slf4s_2.9.1" % "1.0.7"

libraryDependencies += "org.slf4j" % "slf4j-api" % "1.7.2"

libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.0.9"