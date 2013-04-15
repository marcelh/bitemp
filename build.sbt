
name := "bitemp"

version := "0.4"

scalaVersion := "2.10.1"

scalacOptions ++= Seq("-deprecation", "-unchecked", "-feature")

//====================================================================================================================
// Java dependencies: 

libraryDependencies += "joda-time" % "joda-time" % "2.1" withSources()

libraryDependencies += "org.joda" % "joda-convert" % "1.2"

libraryDependencies += "junit" % "junit" % "4.10"

libraryDependencies += "org.slf4j" % "slf4j-api" % "1.7.2"

libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.0.9"

libraryDependencies += "com.typesafe" % "config" % "1.0.0"

//====================================================================================================================
// Scala dependencies: 

libraryDependencies += "org.scalatest" % "scalatest_2.10.0" % "2.0.M5" % "test"

libraryDependencies += "org.mongodb" %% "casbah" % "2.5.0"

libraryDependencies += "org.clapper" %% "grizzled-slf4j" % "1.0.1"

libraryDependencies += "nl.grons" %% "metrics-scala" % "2.2.0" withSources()

libraryDependencies += "com.github.scala-incubator.io" %% "scala-io-core" % "0.4.2"

libraryDependencies += "com.github.scala-incubator.io" %% "scala-io-file" % "0.4.2"

libraryDependencies += "com.jsuereth" %% "scala-arm" % "1.3"

//====================================================================================================================

