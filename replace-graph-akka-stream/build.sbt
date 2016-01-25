name := "akka-stream"

scalaVersion := "2.11.7"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-stream-experimental" % "2.0.2",
  "com.typesafe.akka" %% "akka-http-experimental" % "2.0.2",
  "org.rogach" %% "scallop" % "0.9.5"
)

cancelable in Global := true