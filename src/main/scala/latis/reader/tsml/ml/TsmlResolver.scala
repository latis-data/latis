package latis.reader.tsml.ml

import java.io.File
import java.io.FileNotFoundException
import java.net.URL

import latis.util.LatisProperties
import latis.util.FileUtils

/**
 * Provides access to a tsml from a url, path, or name. 
 */
object TsmlResolver {
  
  def fromUrl(url: URL): Option[Tsml] = Option(Tsml(url))
    
  //TODO: reconcile with other methods like this (LATIS-619)
  def makeUrl(path: String): Option[URL] = Option {
    if (path.contains(":")) new URL(path) //already absolute with a scheme
    else if (path.startsWith(File.separator)) new URL("file:" + path) //absolute file path
    else getClass.getResource("/"+path) match { //try in the classpath (e.g. "resources")
      case url: URL => 
        //Test if this resolves. Note that this only checks file URLs, for now.
        if (FileUtils.exists(url)) url
        else null
      case null => {
        //Try looking in the working directory.
        //Make sure it exists, otherwise this would become a catch-all
        val file = scala.util.Properties.userDir + File.separator + path
        if (new File(file).exists) new URL("file:" + file)  //TODO: use java7 Files
        else null
      }
    }
  }
  
  def fromPath(path: String): Option[Tsml] = {
    makeUrl(path).flatMap(u => fromUrl(u))
  }

  def fromName(name: String): Option[Tsml] = {
    val path = LatisProperties.getOrElse("dataset.dir", "datasets") + 
               File.separator + name + ".tsml"
    fromPath(path)
  }

}