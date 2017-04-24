package latis.reader.tsml.ml

import java.io.File
import java.io.FileNotFoundException
import java.net.URL

import latis.util.LatisProperties

/**
 * Provides access to a tsml from a url, path, or name. 
 */
object TsmlResolver {
  
  def fromUrl(url: URL): Tsml = Tsml(url)
    
  def fromPath(path: String): Tsml = {
    val url = if (path.contains(":")) path //already absolute with a scheme
    else if (path.startsWith(File.separator)) "file:" + path //absolute file path
    else getClass.getResource("/"+path) match { //try in the classpath (e.g. "resources")
      case url: URL => url.toString
      case null => {
        //Try looking in the working directory.
        //Make sure it exists, otherwise this would become a catch-all
        val file = scala.util.Properties.userDir + File.separator + path
        if (new File(file).exists) "file:" + file  //TODO: use java7 Files
        else null
      }
    }
    
    if (url != null) fromUrl(new URL(url))
    else throw new FileNotFoundException(s"Could not resolve path: '$path'.")
  }

  def fromName(name: String): Tsml = {
    val path = LatisProperties.getOrElse("dataset.dir", "datasets") + 
               File.separator + name + ".tsml"
    fromPath(path)
  }

}