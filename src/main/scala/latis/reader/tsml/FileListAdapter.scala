package latis.reader.tsml

import latis.reader.tsml.ml.Tsml
import scala.collection._
import java.io.File

/**
 * Return a list of files as a Dataset.
 */
class FileListAdapter(tsml: Tsml) extends GranuleAdapter(tsml) {
  //TODO: see java 7 DirectoryStream...
  
  //assume time -> text
  
  val dir: String = getProperty("directory").get
  
  def readData: immutable.Map[String, immutable.Seq[String]] = {
    val files = (new File(dir)).list
    
    val times = (0 until files.length).map(_.toString)
    
    
    
    ???
  }
  
  override def close = {}
}