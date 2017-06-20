name := "cmwell-massive-graph-replace"

version := "1.0"

scalaVersion := "2.12.2"

cancelable in Global := true

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-stream" % "2.5.3",
  "com.typesafe.akka" %% "akka-http-core" % "10.0.7",
  "com.typesafe.akka" %% "akka-stream-contrib" % "0.8",
  "org.rogach" %% "scallop" % "3.0.1",
  "org.scalatest" %% "scalatest" % "3.0.1" % "test"
)
