package transfer

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

class CRC32FaultyCalculator(store: Store[IO]) {
  private val CHUNK_SIZE = 8192

  def computeMD5(path: Path)(implicit concurrent: Concurrent[IO]): IO[Long] = {
    val crc32 = new CRC32()
    fs2.Stream
      .eval(IO(println(s"Computing CRC32 for $path")))
      .flatMap { _ =>
        store
          .get(path, CHUNK_SIZE)
          .chunks
          .broadcastThrough(crc32Pipeline(crc32))
      }
      .compile
      .drain
      .map(_ => crc32.getValue)
      //.as(crc32.getValue)
  }

  private def crc32Pipeline: CRC32 => fs2.Pipe[IO, Chunk[Byte], Unit] =
    crc32 => { stream =>
      stream.map { chunk =>
        val bytes = chunk.toBytes
        crc32.update(bytes.values, 0, bytes.size)
        //println(s"Currently having ${crc32.getValue}")
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

    new CRC32FaultyCalculator(localStore)
      .computeMD5(Path("Docker.dmg"))
      .flatMap { v =>
        IO(println(s"CRC value is $v"))
      }
      .as(ExitCode.Success)
  }
}
