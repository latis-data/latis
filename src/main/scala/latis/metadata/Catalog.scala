package latis.metadata

import latis.util.LatisProperties
import java.net.URL
import java.io.File

/**
 * Stand-in for a catalog of datasets that return the URL of the TSML descriptor 
 * for a dataset by name. Used by server.
 */
object Catalog {
  
  private lazy val dsdir: String = LatisProperties.getOrElse("dataset.dir", "datasets")
  
  private def toUrl(path: String): URL = {
    if (path.contains(":")) new URL(path)
    else if(path.startsWith(File.separator)) new URL(s"file:$path")
    else new URL("file:" + LatisProperties.resolvePath(path))
  }
  
  private def getExtension(f: File): String = {
    val nameChunks = f.getName().split('.')
    if (nameChunks.length > 1) {
      nameChunks.last
    }
    else {
      ""
    }
  }
  
  def getTsmlUrl(dataset: String): URL = {
    val tsml = dsdir + File.separator + dataset + ".tsml"
    toUrl(tsml)
  }
}