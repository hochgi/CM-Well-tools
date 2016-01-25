package cmwell.agents.replacegraph

import scala.concurrent._
import scala.concurrent.duration.FiniteDuration

/**
 * Created by gilad on 1/25/16.
 */
object FutureUtils {
  /**
   * @param maxRetries max numbers to retry the task
   * @param waitBetweenRetries optional "cool-down" wait period
   * @param task the task to run
   */
  def retry[T](maxRetries: Int, waitBetweenRetries: Option[FiniteDuration] = None)(task: => Future[T])(implicit ec: ExecutionContext): Future[T] = {
    require(maxRetries > 0, "maxRetries must be positive")
    if (maxRetries > 1) task.recoverWith {
      case t: Throwable => waitBetweenRetries match {
        case None => retry(maxRetries - 1)(task)
        case Some(waitTime) => Future[Unit]{
          try {
            Await.ready(Promise().future, waitTime)
          }
          catch {
            case _: TimeoutException => ()
          }
        }.flatMap(_ => retry(maxRetries - 1, waitBetweenRetries)(task))
      }
    } else task
  }
}
