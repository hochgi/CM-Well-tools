package cmwell.agents.replacegraph

import akka.actor.ActorSystem
import akka.http.scaladsl.model.{HttpResponse, HttpRequest}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Keep, Source}
import org.rogach.scallop.ScallopConf
import akka.http.scaladsl._
import scala.concurrent.duration._
import scala.concurrent.{Future, Await}
import scala.concurrent.ExecutionContext.Implicits.global

/**
 * Created by gilad on 1/21/16.
 */
object GraphReplace extends App {

  object Conf extends ScallopConf(args) {
    version("CM-Well Graph Delete Tool 0.0.1")
    lazy val parFactor = scala.math.min(sys.runtime.availableProcessors * 4,32)
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
  val par = Conf.par()
  val lh = Conf.lh()

  implicit val system = ActorSystem("akka-stream-replace-graph")
  implicit val materializer = ActorMaterializer()

  val posFut: Future[String] = Conf.pos.get match {
    case Some(pos) => Future.successful(pos)
    case None => {
      val path = Conf.path()
      val qp = Conf.qp.get
      val rec = Conf.rec()
      val uriToQuery = s"http://$host:$port$path?op=create-consumer&format=text${qp.map("&qp=" + _).getOrElse("")}${if (rec) "&with-descendants" else ""}"
      val request = HttpRequest(uri = uriToQuery)

      FutureUtils.retry(5, Some(2.seconds)) {
        Http().singleRequest(request).map {
          case res@HttpResponse(s, h, e, p) if s.isSuccess() => {
            e.dataBytes.runWith(Sink.ignore)
            h.find(_.name() == "X-CM-WELL-POSITION") match {
              case Some(pos) => pos.value()
              case None => {
                val msg = s"No position header in response: $res"
                System.err.println(msg)
                throw new RuntimeException(msg)
              }
            }
          }
          case badRes => {
            val msg = s"create-consumer failed: $badRes"
            System.err.println(msg)
            throw new RuntimeException(msg)
          }
        }
      }
    }
  }

  val srcFut = posFut.map(Source.unfoldAsync(_){ pos =>
    println(s"next position: $pos")
    val req = HttpRequest(uri = s"http://$host:$port/?op=consume&position=$pos&format=text&length-hint=$lh")
    FutureUtils.retry(5, Some(2.seconds)) {
      Http().singleRequest(req).map {
        case res@HttpResponse(s, h, e, _) if s.isSuccess() => h.find(_.name() == "X-CM-WELL-POSITION") match {
          case None if s.intValue() != 204 => {
            val msg = s"No position header in response: $res"
            System.err.println(msg)
            throw new RuntimeException(msg)
          }
          case opt => opt.map(_.value() -> e.dataBytes)
        }
        case consumeRes => {
          val msg = s"Bad response from consume: $consumeRes\nFrom request: $req"
          System.err.println(msg)
          throw new RuntimeException(msg)
        }
      }
    }
  }.flatMapConcat(identity))

  try {
    val con = Http().outgoingConnection(host,port)
    val f = srcFut.flatMap{_.toMat(SourceUtils.replaceGraphs(con,par))(Keep.right).run()}
    Await.ready(f, Duration.Inf)
  } finally {
    materializer.shutdown()
    system.shutdown()
  }
}
