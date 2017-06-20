import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.model._
import akka.http.scaladsl.settings.ConnectionPoolSettings
import akka.{Done, NotUsed}
import akka.stream.Materializer
import akka.stream.contrib.{Retry, SourceGen}
import akka.stream.scaladsl.{Flow, Framing, Keep, RunnableGraph, Sink, Source}
import akka.util.ByteString
import scala.collection.breakOut
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success, Try}

/**
  * Proj: cmwell-massive-graph-replace
  * User: gilad
  * Date: 6/20/17
  * Time: 11:22 AM
  */
object ReplaceGraphIngester {

  val endln = ByteString("\n")
  lazy val responseNotOK = new Exception("responseNotOK")

  def build(protocol: String, host: String, port: Int, par: Int, lh: Int, token: Option[String], pathToStatements: Map[String, Vector[String]], graphs: Set[String])
           (implicit ctx: ExecutionContext, sys: ActorSystem, mat: Materializer): Future[RunnableGraph[Future[Done]]] = {

    val byteStringToStatements: ByteString => Vector[String] = bs => {
      val path = bs.utf8String
      val markReplace = markReplaceFromPath(path,graphs)
      val statements = pathToStatements.getOrElse(path, Vector.empty[String])
      statements ++ markReplace
    }

    createConsumer(protocol, host, port, mkQP(graphs), lh).map{ initialPosition =>

      val pathsSource = consumeSource(protocol, host, port, lh, initialPosition: String)

      pathsSource
        .via(Framing.delimiter(endln,65536))
        .map(byteStringToStatements)
        .batchWeighted(2048,_.length,identity)(_ ++ _)
        .toMat(ingestStatementsSink(protocol,host,port,par,token))(Keep.right)
    }
  }

  def ingestStatementsSink(protocol: String, host: String, port: Int, par: Int, token: Option[String])
                          (implicit ctx: ExecutionContext, sys: ActorSystem, mat: Materializer) : Sink[Vector[String],Future[Done]] = {
    val con = Retry(getConnectionPool[(HttpRequest,Int)](protocol,host,port)){
      case (_, 0) => None
      case (req, retriesLeft) =>  Some(req -> ((req, retriesLeft - 1)))
    }
    val flw = Flow.fromFunction[Vector[String],(HttpRequest,(HttpRequest,Int))]{ vec =>
      val entity: Source[ByteString,NotUsed] = Source(vec).map(ByteString(_)).intersperse(endln)
      val req = HttpRequest(
        method = HttpMethods.POST,
        uri = s"$protocol://$host:$port/_in?format=nquads",
        headers = token.fold(List.empty[HttpHeader])(v => List(RawHeader("X-CM-WELL-TOKEN", v))),
        entity = HttpEntity(ContentTypes.NoContentType,entity))
      req -> (req -> 5)
    }
    flw.via(con).toMat(Sink.foreach {
      case (tryRes,_) => if(tryRes.isFailure || tryRes.get.status.isFailure())
        System.err.println("failed request:\n" + tryRes)
    })(Keep.right)
  }

  def getConnectionPool[T](protocol: String, host: String, port: Int)
                          (implicit ctx: ExecutionContext, sys: ActorSystem, mat: Materializer) = (protocol match {
    case "http" => Http().newHostConnectionPool[T](host, port, ConnectionPoolSettings("akka.http.host-connection-pool.max-connections=1"))
    case "https" => Http().newHostConnectionPoolHttps[T](host, port, settings = ConnectionPoolSettings("akka.http.host-connection-pool.max-connections=1"))
    case unknown => throw new RuntimeException(s"I don't know what to do with protocol of type [$unknown]")
  }).mapAsync(1){
    case (Success(res),tup) if res.status.isFailure() => {
      val p = Promise[(Try[HttpResponse],T)]()
      res.entity.dataBytes.runWith(Sink.ignore).onComplete(_ => p.success(Failure[HttpResponse](responseNotOK) -> tup))
      p.future
    }
    case x => Future.successful(x)
  }

  def consumeSource(protocol: String, host: String, port: Int, lh: Int, initialPosition: String)
                   (implicit ctx: ExecutionContext, sys: ActorSystem, mat: Materializer): Source[ByteString, NotUsed] = {
    val req = (pos: String) => HttpRequest(uri = s"$protocol://$host:$port/_consume?position=$pos&format=text&length-hint=$lh")
    val con = Retry(getConnectionPool[(String,Int)](protocol,host,port)){
      case (_, 0) => None
      case (position, retriesLeft) =>  Some(req(position) -> ((position, retriesLeft - 1)))
    }
    val flw = Flow.fromFunction[String,(HttpRequest,(String,Int))](pos => req(pos) -> ((pos,5))).via(con).map{
      case (Success(HttpResponse(s, h, e, p)),(oldPOsition,_)) if s.isSuccess() =>
        val state = h.find(_.name() == "X-CM-WELL-POSITION").fold(oldPOsition)(_.value())
        val element = e.dataBytes
        state -> element
    }
    val src = SourceGen.unfoldFlow(initialPosition)(flw)
    src.flatMapConcat(x => x)
  }

  def createConsumer(protocol: String, host: String, port: Int, qp: String, lh: Int)
                    (implicit ctx: ExecutionContext, sys: ActorSystem, mat: Materializer): Future[String] = {
    val uriToQuery = s"$protocol://$host:$port/?op=create-consumer&format=text&with-descendants&$qp&length-hint=$lh"
    val request = HttpRequest(uri = uriToQuery)
    Http().singleRequest(request).map {
      case res@HttpResponse(s, h, e, p) if s.isSuccess() => {
        e.dataBytes.runWith(Sink.ignore)
        h.find(_.name() == "X-CM-WELL-POSITION") match {
          case Some(pos) => pos.value()
          case None => {
            val msg = s"No position header in response: $res"
            throw new RuntimeException(msg)
          }
        }
      }
      case badRes => {
        val msg = s"create-consumer failed: $badRes"
        throw new RuntimeException(msg)
      }
    }
  }

  def mkQP(graphs: Set[String]): String = graphs.mkString("qp=*system.quad::", ",*system.quad::", "")

  val markReplace = "> <cmwell://meta/sys#markReplace> <*> <"
  val endStmt = "> ."

  def markReplaceFromPath(path: String, graphs: Set[String]): Vector[String] = {
    val subject = "<" + cmwellPathToURI(path)
    graphs.map(q => subject + markReplace + q + endStmt)(breakOut)
  }

  def cmwellPathToURI(path: String): String = {
    if (path.startsWith("/https.")) "https://" + path.drop("/https.".size)
    else if (path.tail.takeWhile('/'.!=).contains('.')) "http:/" + path
    else "cmwell:/" + path
  }
}