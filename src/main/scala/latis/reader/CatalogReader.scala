package latis.reader

import java.io.File
import java.net.URI
import java.net.URL
import java.net.URLDecoder
import java.net.URLEncoder
import java.nio.file.FileVisitResult
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.SimpleFileVisitor
import java.nio.file.attribute.BasicFileAttributes
import scala.Ordering
import scala.collection.mutable.ArrayBuffer
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
  lazy val dir = URLDecoder.decode({
    val uri = new URI(URLEncoder.encode(loc, "utf-8"))
    if (uri.isAbsolute) uri.toURL //starts with "scheme:...", note this could be file, http, ...
    else if (loc.startsWith(File.separator)) new URL("file:" + loc) //absolute path
    else getClass.getResource("/"+loc) match { //relative path: try looking in the classpath
      case url: URL => url
      case null => new URL("file:" + scala.util.Properties.userDir + File.separator + loc) //relative to current working directory 
    }
  }.getPath, "utf-8")
  
  //samples from which this dataset's function will be constructed
  var samples = ArrayBuffer[Sample]()
  
  private class FileVisitDelegator extends SimpleFileVisitor[Path] {
    
    //model a file as a catalog entry
    override def visitFile(path: Path, attrs: BasicFileAttributes) = {
      if(path.toString.endsWith(".tsml")) {
        val name = path.toString.drop(dir.length + 1).stripSuffix(".tsml")
        samples += Sample(Text(Metadata("name"), name), Tuple(Text(Metadata("description"), ""),
            Tuple(Text(Metadata("accessURL"),name), Metadata("distribution"))))
        FileVisitResult.CONTINUE
      } else FileVisitResult.CONTINUE
    }
    
    //model a directory as a nested catalog
    override def preVisitDirectory(path: Path, attrs: BasicFileAttributes) = {
      if(path.toString != dir) {
        //recurse to created a nested function
        val ds = CatalogReader(path.toString).getDataset()
        ds match {
          case Dataset(Function(f)) if(f.isEmpty) => //drop empty catalogs
          case Dataset(v) => samples += Sample(Text(Metadata("name"), "foobar"/*ds.getName.drop(dir.length + 1)*/), v)
          case _ => //empty dataset? 
        }
        FileVisitResult.SKIP_SUBTREE
      } else FileVisitResult.CONTINUE
    }
  }
  
  def getDataset(operations: Seq[Operation]): Dataset = {
    Files.walkFileTree(Paths.get(dir), new FileVisitDelegator)
    val sorted = samples.sortBy(s => s.domain match {
      case Text(str) => str
    })(Ordering.String)
    val f = Function(sorted, Metadata("datasets"))
    val dataset = Dataset(f, Metadata(loc))
    operations.foldLeft(dataset)((ds,op) => op(ds))
  }
  
  def close = {}
  
}

object CatalogReader {
  
  def apply() = new CatalogReader
  
  def apply(path: String) = new CatalogReader(path)
}
