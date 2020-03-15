package transfer

import java.nio.file.Paths
import java.util.concurrent.Executors
import java.util.zip.CRC32

import blobstore.fs.FileStore
import blobstore.{Path, Store}
import cats.data.Kleisli
import cats.effect.concurrent.MVar
import cats.effect.{Blocker, Concurrent, ExitCode, IO, IOApp}
import cats.implicits._
import fs2.Chunk

import scala.concurrent.ExecutionContext

class CRC32Calculator(store: Store[IO]) {
  private val CHUNK_SIZE = 8192

  def computeMD5(path: Path)(implicit concurrent: Concurrent[IO]): IO[Long] = {
    val mvar = MVar.of(new CRC32())
    fs2.Stream
      .eval(IO(println(s"Computing CRC32 for $path")))
      .flatMap(_ => fs2.Stream.eval(mvar))
      .flatMap { mvar =>
        store
          .get(path, CHUNK_SIZE)
          .chunks
          .broadcastThrough(crc32Pipeline(mvar))
      }
      .compile
      .lastOrError
      .flatMap(_ => mvar.flatMap(r => r.take.map(_.getValue)))
  }

  private def crc32Pipeline: MVar[IO, CRC32] => fs2.Pipe[IO, Chunk[Byte], Unit] =
    crc32 => { stream =>
      stream
        .flatMap { chunk =>
          fs2.Stream.eval(for {
            bytes <- IO(chunk.toBytes)
            algorithm <- crc32.take
            _ <- IO(algorithm.update(bytes.values, 0, bytes.size))
            _ <- crc32.put(algorithm)
          } yield ())
        }
    }
}

object Main extends IOApp {
  override def run(args: List[String]): IO[ExitCode] = {
    val executionContext =
      ExecutionContext.fromExecutor(Executors.newFixedThreadPool(1))
    val blocker = Blocker.liftExecutionContext(executionContext)
    val localStore = new FileStore[IO](Paths.get(""), blocker)

    import blobstore.PathOps._

    new CRC32Calculator(localStore)
      .computeMD5(Path("Docker.dmg"))
      .flatMap { v =>
        IO(println(s"CRC value is $v"))
      }
      .as(ExitCode.Success)
  }
}
