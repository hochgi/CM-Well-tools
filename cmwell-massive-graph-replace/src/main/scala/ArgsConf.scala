import org.rogach.scallop._

/**
  * Proj: hello-akka
  * User: gilad
  * Date: 6/20/17
  * Time: 10:23 AM
  */
class ArgsConf (arguments: Seq[String]) extends ScallopConf(arguments) {

  version("CM-Well Graph Delete Tool 0.0.1")
  val protocol = opt[String](name = "protocol", noshort = true, required = false, default = Some("http"))
  val host = opt[String](name = "host", short = 'h', required = true, descr = "CM-Well host to operate on (domain or IP with no port)")
  val port = opt[Int](name = "port", noshort = true, default = ArgsConf.defaultPortForProtocol(protocol()), descr = "CM-Well host's port")
  val lh = opt[Int](name = "length-hint", noshort = true, default = Some(64), descr = "Set 'length-hint' parameter to pass 'consume' operation")
  val par = opt[Int](name = "parallelism", noshort = true, default = Some(ArgsConf.parFactor), descr = "Set the amount of the allowed parallel connections")
  val enc = opt[String](name = "encoding", short = 'e', required = false, default = Some("UTF-8"), descr = "Encoding of input file")
  val token = opt[String](name = "token", short = 't', required = false, descr = "Auth writing token")
  val filename = trailArg[String](required = true, descr = "NQuads file with replace graph statements")
  verify()
}

object ArgsConf {

  lazy val parFactor: Int = scala.math.min(sys.runtime.availableProcessors * 4,32)

  def defaultPortForProtocol(protocol: String): Option[Int] = protocol match {
    case "http" => Some(80)
    case "https" => Some(443)
    case _ => None
  }
}