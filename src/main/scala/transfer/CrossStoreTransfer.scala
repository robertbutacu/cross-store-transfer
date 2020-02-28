package transfer

import java.io.{BufferedWriter, FileWriter}
import java.nio.file.Paths

import blobstore.fs.FileStore
import blobstore.{Path, Store}
import cats.{Monad, MonadError}
import cats.effect.concurrent.Ref
import cats.effect.{Blocker, ExitCode, IO, IOApp}
import transfer.CrossStoreTransfer.ThrowableMonadError

import scala.concurrent.ExecutionContext.Implicits
import scala.language.higherKinds

class CrossStoreTransfer[F[_]: ThrowableMonadError](sourceStore: Store[F],
                                                    destinationStore: Store[F],
                                                    byteCounter: Ref[F, Long]) {
  import cats.syntax.all._

  def withSafeTransfer(
    path: Path
  ): fs2.Stream[F, Unit] => fs2.Stream[F, Unit] = { f =>
    f.recoverWith {
      case e: Throwable =>
        fs2.Stream.eval {
          println(s"OOuupsie daisy - will delete file - encountered error $e").pure >> destinationStore
            .remove(path)
        }
    }
  }

  def transfer(from: Path, to: Path): fs2.Stream[F, Unit] = {
    sourceStore
      .get(from, 4096)
      .through(countByte)
      .through(logger)
      .through(maybeRaiseError)
      .through(destinationStore.put(to))
  }

  private def countByte: fs2.Stream[F, Byte] => fs2.Stream[F, Byte] = {
    stream =>
      stream.flatMap(
        byte => fs2.Stream.eval(byteCounter.update(_ + 1)).map(_ => byte)
      )
  }

  private def logger: fs2.Stream[F, Byte] => fs2.Stream[F, Byte] = { stream =>
    stream.flatMap(
      byte =>
        fs2.Stream
          .eval(byteCounter.get.flatMap { bytesRead =>
            if (bytesRead % 4096 == 0)
              println(s"Read 4096 more bytes: currently at $bytesRead").pure
            else ().pure

          })
          .map(_ => byte)
    )
  }

  private def maybeRaiseError: fs2.Stream[F, Byte] => fs2.Stream[F, Byte] = {
    _.flatTap { _ =>
      fs2.Stream.eval[F, Unit] {
        byteCounter.get.flatMap { read =>
          if (read == 100000)
            MonadError[F, Throwable].raiseError(
              new RuntimeException("Ouupsie daisy")
            )
          else ().pure
        }
      }
    }
  }
}

object CrossStoreTransfer extends IOApp {
  type ThrowableMonadError[F[_]] = MonadError[F, Throwable]
  override def run(args: List[String]): IO[ExitCode] = {
    for {
      byteCounter <- Ref.of[IO, Long](0L)
      blocker = Blocker.liftExecutionContext(Implicits.global)
      source = new FileStore[IO](Paths.get(""), blocker)
      destination = new FileStore[IO](Paths.get(""), blocker)
      crossStoreTransfer = new CrossStoreTransfer[IO](
        source,
        destination,
        byteCounter
      )
      _ <- crossStoreTransfer
        .transfer(Path("Docker.dmg"), Path("docker-copy.dmg"))
        .through(crossStoreTransfer.withSafeTransfer(Path("docker-copy.dmg")))
        .compile
        .drain
    } yield ExitCode.Success
  }
}
