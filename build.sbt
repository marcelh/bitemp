
name := "bitemp"

version := "0.1"

scalaVersion := "2.9.2"

scalacOptions ++= Seq("-deprecation", "-unchecked")

libraryDependencies += "joda-time" % "joda-time" % "2.1" withSources()

libraryDependencies += "org.joda" % "joda-convert" % "1.2"

libraryDependencies += "org.scalatest" %% "scalatest" % "2.0.M5"

libraryDependencies += "junit" % "junit" % "4.10"

libraryDependencies += "org.mongodb" %% "casbah" % "2.4.1"

libraryDependencies += "com.weiglewilczek.slf4s" % "slf4s_2.9.1" % "1.0.7"

libraryDependencies += "org.slf4j" % "slf4j-api" % "1.7.2"

libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.0.9"

libraryDependencies += "org.mockito" % "mockito-all" % "1.9.5"

//libraryDependencies += "org.scalamock" %% "scalamock-scalatest-support" % "latest.integration"

libraryDependencies += "com.yammer.metrics" % "metrics-scala_2.9.1" % "2.2.0" withSources()