package latis.bench

import java.io.IOException
import java.nio.file.FileVisitResult
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.SimpleFileVisitor
import java.nio.file.attribute.BasicFileAttributes
import java.util.concurrent.TimeUnit

import latis.util.FileUtils
import latis.util.FileUtilsNio
import org.openjdk.jmh.annotations._

@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
class FileList {

  @Benchmark
  def fileList: Seq[String] =
    FileUtils.listAllFiles(root.toString, false)

  @Benchmark
  def fileListNIO: Seq[String] =
    FileUtilsNio.listAllFiles(root.toString, false)

  @Benchmark
  def fileListSize: Seq[String] =
    FileUtils.listAllFiles(root.toString, true)

  @Benchmark
  def fileListNIOSize: Seq[String] =
    FileUtilsNio.listAllFiles(root.toString, true)

  var root: Path = _

  @Setup
  def setup(): Unit = {
    root = Files.createTempDirectory(null)

    (1947 to 2018).foreach { year =>
      val yearDir = Files.createDirectory(root.resolve(s"$year"))

      (1 to 356).foreach { day =>
        Files.createFile(yearDir.resolve(f"file_$day%03d"))
      }
    }
  }

  @TearDown
  def clean(): Unit = {
    val visitor = new SimpleFileVisitor[Path] {
      override def visitFile(
        file: Path,
        attrs: BasicFileAttributes
      ): FileVisitResult = {
        Files.delete(file)
        FileVisitResult.CONTINUE
      }

      override def postVisitDirectory(
        dir: Path,
        ex: IOException
      ): FileVisitResult = {
        Files.delete(dir)
        FileVisitResult.CONTINUE
      }
    }

    Files.walkFileTree(root, visitor)
  }
}
