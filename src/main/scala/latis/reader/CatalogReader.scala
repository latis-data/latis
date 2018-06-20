package latis.reader

import java.io.File
import java.net.URI
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file._
import scala.Ordering
import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConverters._
import latis.dm.Dataset
import latis.dm.Function
import latis.dm.Sample
import latis.dm.Text
import latis.dm.Tuple
import latis.metadata.Metadata
import latis.util.LatisProperties
import latis.ops.Operation

/**
 * Create a catalog from a given directory which defaults to the 'dataset.dir' property
 * which defaults to 'datasets'. Nested directories will be treated as nested catalogs.
 *
 * loc is assumed to be either an absolute path on the file system
 * (starting with a file separator) or a relative path. If it is a
 * relative path, we will first try to find it on the classpath and
 * then look in the current working directory.
 */
class CatalogReader(val loc: String) extends DatasetAccessor {
  
  /*
   * Default constructor to allow reflection to work
   */
  def this() = {
    this(LatisProperties.getOrElse("dataset.dir", "datasets"))
  }
  
  /**
   * The path of 'loc'
   */
  lazy val dir: URI =
    if (loc.startsWith(File.separator)) {
      // Absolute path on filesystem
      new URI("file", loc, null)
    } else {
      // On classpath
      Option(getClass.getResource(s"${File.separator}$loc")).map(_.toURI)
        .getOrElse {
          // Relative to working directory
          val userDir = scala.util.Properties.userDir
          new URI("file", s"$userDir${File.separator}$loc", null)
        }
    }
  
  //samples from which this dataset's function will be constructed
  var samples = ArrayBuffer[Sample]()
  
  private class FileVisitDelegator extends SimpleFileVisitor[Path] {
    
    //model a file as a catalog entry
    override def visitFile(path: Path, attrs: BasicFileAttributes): FileVisitResult = {
      if(path.toString.endsWith(".tsml")) {
        val name = path.getFileName.toString.stripSuffix(".tsml")
        samples += Sample(Text(Metadata("name"), name), Tuple(Text(Metadata("description"), ""),
            Tuple(Text(Metadata("accessURL"),name), Metadata("distribution"))))
        FileVisitResult.CONTINUE
      } else FileVisitResult.CONTINUE
    }
    
    //model a directory as a nested catalog
    override def preVisitDirectory(path: Path, attrs: BasicFileAttributes): FileVisitResult = {
      if(path.toUri().equals(dir)) {
        //recurse to created a nested function
        val ds = CatalogReader(path.toString).getDataset()
        ds match {
          case Dataset(Function(f)) if(f.isEmpty) => //drop empty catalogs
          case Dataset(v) => samples += Sample(Text(Metadata("name"), ds.getName.drop(dir.toString.length + 1)), v)
          case _ => //empty dataset? 
        }
        FileVisitResult.SKIP_SUBTREE
      } else FileVisitResult.CONTINUE
    }
  }
  
  def getDataset(operations: Seq[Operation]): Dataset = {
    if (dir.getScheme == "jar") {
      val (jarUri, dsPath) =
        dir.getSchemeSpecificPart.split('!').toList match {
          case u :: p :: Nil => (new URI(u), p)
          case _             => throw new RuntimeException(
            "JAR URI missing path"
          )
        }

      withFileSystem(Paths.get(jarUri)) { fs =>
        // The FileSystem object must remain open while walking the
        // file tree.
        Files.walkFileTree(fs.getPath(dsPath), new FileVisitDelegator)
      }
    } else {
      Files.walkFileTree(Paths.get(dir), new FileVisitDelegator)
    }

    val sorted = samples.sortBy(s => s.domain match {
      case Text(str) => str
    })(Ordering.String)
    val f = Function(sorted, Metadata("datasets"))
    val dataset = Dataset(f, Metadata(loc))
    operations.foldLeft(dataset)((ds,op) => op(ds))
  }

  /**
   * Perform an operation using a FileSystem object.
   *
   * This will create a FileSystem object around the file given by
   * `path`, apply `f` to that FileSystem, and then close the
   * FileSystem.
   *
   * @param path Path to file
   * @param f Operation to perform using a FileSystem
   * @return the result of `f`
   */
  private def withFileSystem[A](path: Path)(f: FileSystem => A): A = {
    val fs = FileSystems.newFileSystem(path, null)
    val ret = f(fs)
    fs.close()
    ret
  }
  
  def close: Unit = {}
  
}

object CatalogReader {
  
  def apply(): CatalogReader = new CatalogReader
  
  def apply(path: String): CatalogReader = new CatalogReader(path)
}
