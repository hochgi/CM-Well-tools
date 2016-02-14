name := "simple-replace-graph"

scalaVersion := "2.11.7"

version := "0.0.3"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-stream" % "2.4.2",
  "com.typesafe.akka" %% "akka-http-core" % "2.4.2",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.1.0",
  "ch.qos.logback" % "logback-classic" % "1.1.3",
  "org.codehaus.groovy" % "groovy-all" % "2.4.5" % "runtime",
  "org.rogach" %% "scallop" % "0.9.5"
)

cancelable in Global := true

sourceGenerators in Compile += Def.task {
  val file = (sourceManaged in Test).value / "cmwell" / "agents" / "simplereplacegraph" / "build" / "Info.scala"
  IO.write(file,
    s"""
       |package cmwell.agents.simplereplacegraph.build
       |
       |object Info  {
       |  val buildVersion = "${version.value}"
       |}
     """.stripMargin)
  Seq(file)
}.taskValue

assemblyJarName in assembly := s"simple-replace-graph-tool-${version.value}.jar"

mainClass in assembly := Some("cmwell.agents.simplereplacegraph.Main")
