package latis.reader

import java.io.File
import java.net.URI
import java.net.URL
import java.net.URLDecoder
import java.net.URLEncoder

import latis.data.value.StringValue
import latis.dm.Dataset
import latis.dm.Function
import latis.dm.Text
import latis.dm.Tuple
import latis.metadata.Metadata
import latis.util.DataMapUtils
import latis.util.FileUtilsNio
import latis.util.LatisProperties

/**
 * Creates a catalog of the tsml's found in 'dataset.dir'.
 */
class FlatCatalogReader extends DatasetAccessor {
  
  /**
   * Get the path of 'dataset.dir', defaulting to 'datasets'.
   */
  val dir = URLDecoder.decode({
    val loc = LatisProperties.getOrElse("dataset.dir", "datasets")
    val uri = new URI(URLEncoder.encode(loc,"utf-8"))
    if (uri.isAbsolute) uri.toURL //starts with "scheme:...", note this could be file, http, ...
    else if (loc.startsWith(File.separator)) new URL("file:" + loc) //absolute path
    else getClass.getResource("/"+loc) match { //relative path: try looking in the classpath
      case url: URL => url
      case null => new URL("file:" + scala.util.Properties.userDir + File.separator + loc) //relative to current working directory 
    }
  }.getPath,"utf-8")
  
  lazy val template = Function(Text(Metadata("name")), Tuple(Text(Metadata("description")),
      Tuple(Text(Metadata("accessURL")), Metadata("distribution"))))
  
  def getDataset = {
    val files = FileUtilsNio.listAllFilesWithSize(dir).map(_.takeWhile(_ != ','))
    val names = files.filter(_.endsWith(".tsml")).map(_.stripSuffix(".tsml"))
    val accessUrls = names

    val dataMap = names.zip(accessUrls).map(p => Map("name" -> StringValue(p._1), 
                                                     "accessURL" -> StringValue(p._2),
                                                     "description" -> StringValue("")))
    val f = DataMapUtils.dataMapsToFunction(dataMap.iterator, template)
    Dataset(f, Metadata("catalog"))
  }
  
  def close = {}
  
}

object FlatCatalogReader {
  def apply() = new FlatCatalogReader
}