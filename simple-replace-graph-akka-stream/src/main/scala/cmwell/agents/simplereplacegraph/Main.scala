package cmwell.agents.simplereplacegraph

import akka.actor.ActorSystem
import akka.http.scaladsl._
import akka.http.scaladsl.model._
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Framing, Sink, Source}
import akka.util.ByteString
import com.typesafe.scalalogging.LazyLogging
import org.rogach.scallop.ScallopConf
import scala.concurrent.duration._
import scala.concurrent.{Future, Await}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Success

/**
 * Created by gilad on 1/21/16.
 */
object Main extends App with LazyLogging {

  logger.info("CM-Well Simple Replace Graph: application started! ")

  object Conf extends ScallopConf(args) {
    version("CM-Well Graph Delete Tool " + cmwell.agents.simplereplacegraph.build.Info.buildVersion)
    lazy val parFactor = scala.math.min(sys.runtime.availableProcessors,32)
    val host = opt[String](name = "host", short = 'h', required = true, descr = "CM-Well host to operate on (domain or IP with no port)")
    val port = opt[Int](name = "port", noshort = true, default = Some(80), descr = "CM-Well host's port")
    val path = opt[String](name = "path", short = 'p', default = Some("/"), descr = "The path to operate on (refine operation by path)")
    val qp = opt[String](name = "qp", short = 'q', descr = "Query parameter (qp) to refine the operation")
    val lh = opt[Int](name = "length-hint", noshort = true, default = Some(64), descr = "Set 'length-hint' parameter to pass 'consume' operation")
    val rec = opt[Boolean](name = "with-descendants", short = 'r', descr = "Turn on the 'with-descendants' flag")
    val par = opt[Int](name = "parallelism", noshort = true, default = Some(parFactor), descr = "Set the amount of the allowed parallel connections")
    val pos = opt[String](name = "position", noshort = true, descr = "Instead supplying `path`,`qp`,and `with-descendants`, you may resume the operation from the last known position")

    //you can't use position with any of the following options (it's embedded within the position token)
    conflicts(pos, List(rec,qp,path))
  }

  val host = Conf.host()
  val port = Conf.port()
  val path = Conf.path()
  val par = Conf.par()
  val lh = Conf.lh()

  implicit val system = ActorSystem("akka-stream-mass-delete")
  implicit val materializer = ActorMaterializer()

  val con = Http().superPool[None.type]()

  val posFut: Future[String] = Conf.pos.get match {
    case Some(pos) => Future.successful(pos)
    case None => {
      val path = Conf.path()
      val qp = Conf.qp.get
      val rec = Conf.rec()
      val uriToQuery = s"http://$host:$port$path?op=create-consumer&format=text${qp.map("&qp=" + _).getOrElse("")}${if (rec) "&with-descendants" else ""}"
      val request = HttpRequest(uri = uriToQuery)

      retry(5, Some(2.seconds)) {
        Http().singleRequest(request).map {
          case res@HttpResponse(s, h, e, p) if s.isSuccess() => {
            e.dataBytes.runWith(Sink.ignore)
            h.find(_.name() == "X-CM-WELL-POSITION") match {
              case Some(pos) => pos.value()
              case None => {
                val ex = new RuntimeException(s"No position header in response: $res")
                logger.error("missing initial position",ex)
                throw ex
              }
            }
          }
          case badRes => {
            val ex = new RuntimeException(s"create-consumer failed: $badRes")
            logger.error("bad response while retrieving initial position",ex)
            throw ex
          }
        }
      }
    }
  }

  val bodyPrefix = ByteString("<> <cmwell://meta/sys#replaceGraph> <http:/")
  val bodySuffix  = ByteString("> .\n")
  val endln = ByteString("\n")
  val empty = ByteString("")

  try {
    val f = posFut.flatMap(Source.unfoldAsync(_) { pos =>
      logger.info(s"next position: $pos")
      val req = HttpRequest(uri = s"http://$host:$port$path?op=consume&position=$pos&format=text&length-hint=$lh")
      retry(20, Some(2.seconds)) {
        Source.single(req -> None).via(con).runWith(Sink.head).map {
          case (Success(res@HttpResponse(s, h, e, _)),_) if s.isSuccess() => h.find(_.name() == "X-CM-WELL-POSITION") match {
            case None if s != StatusCodes.NoContent => {
              val ex = new RuntimeException(s"No position header in response: $res")
              logger.error("missing position",ex)
              throw ex
            }
            case _ if s == StatusCodes.NoContent => None
            case opt => opt.map(_.value() -> e.dataBytes)
          }
          case consumeRes => {
            val ex = new RuntimeException(s"Bad response from consume: $consumeRes")
            logger.error(s"request causing a bad response: $req",ex)
            throw ex
          }
        }
      }
    }
      .flatMapConcat(identity)
      .via(Framing.delimiter(endln, 4096))
      .filter(_.nonEmpty)
      .batch(16, bs => {bodyPrefix ++ bs ++ bodySuffix})(_ ++ bodyPrefix ++ _ ++ bodySuffix)
      .mapAsync(par) { payload =>
        logger.trace(s"going to post: \n${payload.utf8String}")
        val req = HttpRequest(
          method = HttpMethods.POST,
          uri = s"http://$host:$port/_in?format=ntriples",
          entity = HttpEntity.apply(ContentTypes.`text/plain(UTF-8)`, payload))
        retry(20, Some(2.seconds)) {
          Source.single(req -> None).via(con).runWith(Sink.head).flatMap {
            case (Success(res@HttpResponse(s, h, e, p)), _) => {
              if (s.isSuccess()) Future.successful(e.dataBytes)
              else {
                val f = e.dataBytes.runFold(empty)(_ ++ _)
                f.onComplete(t => logger.error(s"bad response received (_in deletes): $res\nwith payload: $t"))
                f.map(Source.single)
              }
            }
          }
        }
      }
      .flatMapConcat(identity)
      .runForeach(bs => println(bs.utf8String)))

    Await.ready(f, Duration.Inf)
  }
  finally {
    //akka caches connections for 30 seconds by default
    println("Going down in 30 seconds...")
    Thread.sleep(30001)
    Await.ready(system.terminate(),Duration.Inf)
    materializer.shutdown()
  }
}
