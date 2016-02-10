name := "mass-delete"

scalaVersion := "2.11.7"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-stream" % "2.4.2-RC2",
  "com.typesafe.akka" %% "akka-http-core" % "2.4.2-RC2",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.1.0",
  "ch.qos.logback" % "logback-classic" % "1.1.3",
  "org.rogach" %% "scallop" % "0.9.5"
)

cancelable in Global := true

assemblyJarName in assembly := "mass-delete-tool.jar"

mainClass in assembly := Some("cmwell.agents.massdelete.MassDelete")