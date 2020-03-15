package transfer

import cats.effect.{ExitCode, IO, IOApp}
import fs2.Stream
import cats.effect._
import cats.implicits._
import scala.concurrent.duration._

object Experiment extends IOApp {
  override def run(args: List[String]): IO[ExitCode] = {
    runningStream("stream-1").mergeHaltBoth(runningStream("stream-2")).compile.drain.as(ExitCode.Success)
  }

  def runningStream(name: String): Stream[IO, Unit] = Stream.eval {
    Stream.repeatEval(IO(println(s"In $name")) >> IO.sleep(3.seconds)).take(10).compile.drain
  }
}
