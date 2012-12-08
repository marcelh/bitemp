
name := "bitemp"

version := "0.1"

scalaVersion := "2.9.2"

scalacOptions += "-deprecation"

libraryDependencies += "joda-time" % "joda-time" % "2.1" withSources()

libraryDependencies += "org.joda" % "joda-convert" % "1.2"

//libraryDependencies += "org.specs2" %% "specs2" % "1.12.1" % "test"

libraryDependencies += "org.scalatest" %% "scalatest" % "2.0.M3"

libraryDependencies += "junit" % "junit" % "4.10"

libraryDependencies += "com.mongodb.casbah" % "casbah_2.9.0-1" % "2.1.5.0"