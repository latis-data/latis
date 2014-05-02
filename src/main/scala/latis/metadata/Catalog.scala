package latis.metadata

import latis.util.LatisProperties
import java.net.URL
import java.io.File

/**
 * Stand-in for a catalog of datasets that return the URL of the TSML descriptor 
 * for a dataset by name. Used by server.
 */
object Catalog {
  
  def getTsmlUrl(dataset: String): URL = {
    val dsdir = LatisProperties.getOrElse("dataset.dir", "datasets")
    val tsml = dsdir + File.separator + dataset + ".tsml"
    
    //If tsml path has URL scheme, assume we have a valid url  //TODO: use URI api?
    if (tsml.contains(":")) new URL(tsml)
    //or absolute file path, prepend 'file' scheme
    else if (tsml.startsWith(File.separator)) new URL("file:" + tsml)
    //otherwise try to resolve relative path
    else new URL("file:" + LatisProperties.resolvePath(tsml))
  }
}