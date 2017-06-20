import scala.collection.mutable
import scala.collection.breakOut

/**
  * Proj: cmwell-massive-graph-replace
  * User: gilad
  * Date: 6/20/17
  * Time: 10:46 AM
  */
object NquadsFileParser {

  val StatementRegex = raw"""<([^<>]*)>\s+<([^<>]+)>\s+(<([^<>]+)>|".+"((\^\^<[^<>]+>)|@\S+)?)\s+(<([^<>]+)>\s+)?[.]\s*""".r

  def parseFile(filename: String, encoding: String): (Map[String,Vector[String]],Set[String]) = {
    val it = scala.io.Source.fromFile(filename, encoding).getLines()
    val regularStatementsBuilder = mutable.Map.empty[String,mutable.Builder[String,Vector[String]]]
    val graphs = Set.newBuilder[String]
    it.filterNot(_.forall(_.isWhitespace)).foreach {
      case StatementRegex("","cmwell://meta/sys#replaceGraph",_,graph,_,_,_,_) => graphs += graph
      case line @ StatementRegex(subject,_,_,_,_,_,_,_) => regularStatementsBuilder.getOrElseUpdate(getCmwellPath(subject),Vector.newBuilder[String]) += line
    }
    regularStatementsBuilder.mapValues(_.result()).toMap -> graphs.result()
  }

  def getCmwellPath(uri: String): String = {
    if (uri.startsWith("https")) s"/https.${uri.drop("https://".length)}"
    else if (uri.startsWith("cmwell://")) uri.drop("cmwell:/".length)
    else if (uri.startsWith("http:/")) uri.drop("http:/".length)
    else {
      System.err.println(s"value [$uri] has bad prefix, and is not a CM-Well reference (though it is a subject in the original input).")
      uri
    }
  }
}
