import akka.Done
import akka.actor.ActorSystem
import akka.stream.scaladsl.RunnableGraph
import akka.stream.ActorMaterializer
import com.typesafe.scalalogging.LazyLogging

import scala.concurrent.Future
import scala.util.{Failure, Success}

object MassiveGraphReplace extends App  with LazyLogging {

  val conf = new ArgsConf(args)
  val host = conf.host()
  val port = conf.port()
  val lh = conf.lh()
  val enc = conf.enc()
  val protocol = conf.protocol()
  val token = conf.token.toOption
  val file = conf.filename()

  logger.info(s"""Starting tool with configurations: {\n\t\"host\" : \"$host\",\n\t\"port\" : $port,\n\t\"length-hint\" : $lh,\n\t\"file-encoding\" : \"$enc\",\n\t\"protocol\" : \"$protocol\",\n\t${token.fold("")(""""token" : """" + _ + "\",\n\t")}\"file\" : \"$file\"\n}""")

  implicit val ctx = scala.concurrent.ExecutionContext.global
  implicit val sys = ActorSystem("massive-graph-replace")
  implicit val mat = ActorMaterializer.create(sys)

  val (pathToStatements,graphs): (Map[String,Vector[String]],Set[String]) = NquadsFileParser.parseFile(file,enc)
  val runnableAkkaStreamGraphs: Future[RunnableGraph[Future[Done]]] = ReplaceGraphIngester.build(protocol,host,port,lh,token,pathToStatements,graphs)
  runnableAkkaStreamGraphs.flatMap(_.run()).onComplete {
    case Success(Done) => sys.terminate()
    case Failure(error) =>
      System.err.println(error.getMessage)
      System.err.println(stackTraceToString(error))
      sys.terminate()
  }

  def stackTraceToString(t : Throwable): String = {
    val w = new java.io.StringWriter()
    val pw = new java.io.PrintWriter(w)
    t.printStackTrace(pw)
    w.toString
  }
}
