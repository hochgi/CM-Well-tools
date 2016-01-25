package cmwell.agents.replacegraph

import akka.actor.ActorSystem
import akka.http.scaladsl.model.{HttpEntity, HttpMethods, HttpResponse, HttpRequest}
import akka.stream.{FlowShape, Materializer}
import akka.stream.io.Framing
import akka.stream.scaladsl._
import akka.util.ByteString
import scala.concurrent.Future

/**
 * Created by gilad on 11/22/15.
 */
object SourceUtils {

  val bodyPrefix = ByteString("<http:/")
  val bodyInfix  = ByteString("> <cmwell://meta/sys#markReplace> <*> <http:/")
  val bodySuffix = ByteString("> .")
  val colon = ByteString(" : ")
  val endln = ByteString("\n")
  val endlnSource = Source.single(endln)
  val empty = ByteString("")
  val rightSrc = Source.single(Right(empty))
  val mr = ByteString("markReplace | ")
  val xf = ByteString("xfix (fail) | ")
  val mrf = ByteString("markReplace (fail) | ")

  def replaceGraphs(con: Flow[HttpRequest,HttpResponse,_], parallelism: Int)
                   (implicit sys: ActorSystem, mat: Materializer): Sink[ByteString,Future[Unit]] = {

    Flow.fromGraph(GraphDSL.create() { implicit b =>

      import GraphDSL.Implicits._

      val flow = b.add(Framing.delimiter(ByteString("\n"), 4096))
      val balance = b.add(Balance[ByteString](parallelism))
      val outerMerge = b.add(Merge[ByteString](parallelism))
      val framingDelimiter = b.add(Framing.delimiter(ByteString("\n"), 4096))

      // first, search all the infotons associated to this graph
      flow.flatMapConcat { graphPath =>
        val str = graphPath.utf8String
        val qStr = HttpRequest(uri = s"/?op=stream&qp=system.quad::http:/$str&format=text&recursive")
        Source.single(qStr)
          .via(con)
          .flatMapConcat {
            case HttpResponse(s, h, e, _) if s.isSuccess() => {
              h.find(_.name() == "X-CM-WELL-N") match {
                case Some(header) if header.value() != "0" => e.dataBytes
                case opt => {
                  opt match {
                    case None => System.err.println("could not find `X-CM-WELL-N` header for: " + str)
                    case _ => System.err.println ("there is no infotons associated to the graph: " + str)
                  }
                  e.dataBytes.runWith(Sink.ignore)
                  Source.empty[ByteString]
                }
              }
            }
            case HttpResponse(s,h,en,_) =>
              val bs = ByteString("bad response from stream (graph=")
              val rs = ByteString(s"): status=$s, headers=${h.mkString("[",",","]")}, entity=")
              en.dataBytes
                .prepend(Source(List(graphPath,rs)))
                .fold(bs)(_ ++ _)
                .runForeach(System.err.println)
              Source.empty[ByteString]
          }
      } ~> framingDelimiter ~> balance.in

      for (i <- 0 until parallelism) {

        val broadcast = b.add(Broadcast[ByteString](2))
        val zip = b.add(Zip[ByteString,Either[ByteString,ByteString]]())

        balance.out(i).detach ~> broadcast.in
        broadcast.out(0) ~> zip.in0

        //for every path, send a `x-fix` command
        broadcast.out(1).flatMapConcat { path =>
          val pathStr = path.utf8String
          val xfReq = HttpRequest(uri = s"$pathStr/?op=x-fix")
          Source.single(xfReq).via(con).flatMapConcat {
            case HttpResponse(s, _, en, _) if s.isSuccess() =>
              en.dataBytes.runWith(Sink.ignore)
              rightSrc
            case rs =>
              rs.entity
                .dataBytes
                .concat(endlnSource)
                .fold(xf ++ path ++ colon ++ ByteString(rs.status.toString()) ++ colon ++ ByteString(rs.headers.mkString("[",",","]")) ++ colon)(_ ++ _)
                .map(Left.apply)
          }
        } ~> zip.in1

        //for every path, send a `markReplace` command
        zip.out.flatMapConcat {
          //if `x-fix` failed, no point in doing a `markReplace`, so just output the bad response to log
          case (_, Left(xfix)) => Source.single(xfix)
          case (path, Right(_)) => {
            val mrEnt = HttpEntity(bodyPrefix ++ path ++ bodyInfix ++ path ++ bodySuffix)
            val mrReq = HttpRequest(method = HttpMethods.POST, uri = s"/_in?format=nq", entity = mrEnt)
            Source.single(mrReq).via(con).flatMapConcat {
              case HttpResponse(s, _, e, _) if s.isSuccess()=>
                e.dataBytes.concat(endlnSource).fold(mr ++ path ++ colon)(_ ++ _)
              case rs =>
                rs.entity
                  .dataBytes
                  .concat(endlnSource)
                  .fold(mrf ++ path ++ colon ++ ByteString(rs.toString))(_ ++ _)
            }
          }
        } ~> outerMerge.in(i)
      }

      FlowShape(flow.in, outerMerge.out)
    }).toMat(Sink.foreach(x => print(x.utf8String)))(Keep.right)
  }
}