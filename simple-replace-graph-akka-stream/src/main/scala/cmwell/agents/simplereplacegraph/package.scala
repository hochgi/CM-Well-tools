package cmwell.agents

import akka.actor.ActorSystem

import scala.concurrent._
import scala.concurrent.duration.FiniteDuration

/**
 * Created by gilad on 2/10/16.
 */
package object simplereplacegraph {

  def retry[T](maxRetries: Int, waitBetweenRetries: Option[FiniteDuration] = None)(task: => Future[T])(implicit ec: ExecutionContext, sys: ActorSystem): Future[T] = {
    require(maxRetries > 0, "maxRetries must be positive")
    if (maxRetries > 1) task.recoverWith {
      case t: Throwable => waitBetweenRetries match {
        case None => retry(maxRetries - 1)(task)
        case Some(waitTime) => {
          val p = Promise[T]()
          sys.scheduler.scheduleOnce(waitTime){
            p.completeWith(retry(maxRetries - 1, waitBetweenRetries)(task))
          }
          p.future
        }
      }
    } else task
  }

}
