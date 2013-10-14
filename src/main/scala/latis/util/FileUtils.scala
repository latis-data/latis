package latis.util

import java.io.File
import scala.collection.mutable.ArrayBuffer

object FileUtils {

  def listAllFiles(dir: String): Seq[String] = {  
    //don't include "dir" in the names
    
    def accumulateFiles(file: File, buffer: ArrayBuffer[String]) {
      if (file.isDirectory()) file.listFiles().map(accumulateFiles(_, buffer))
      else buffer += file.getPath.drop(dir.length+1)
    }
    
    val buffer = ArrayBuffer[String]()
    accumulateFiles(new File(dir), buffer)
    buffer
  }
  
}