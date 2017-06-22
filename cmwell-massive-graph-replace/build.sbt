name := "cmwell-massive-graph-replace"

version := "1.0"

scalaVersion := "2.12.2"

cancelable in Global := true

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-stream" % "2.5.3",
  "com.typesafe.akka" %% "akka-http-core" % "10.0.7",
  "com.typesafe.akka" %% "akka-stream-contrib" % "0.8",
  "org.rogach" %% "scallop" % "3.0.3",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.5.0",
  "ch.qos.logback" % "logback-classic" % "1.2.3",
  "org.scalatest" %% "scalatest" % "3.0.3" % "test"
)

test in assembly := {}

mainClass in assembly := Some("MassiveGraphReplace")