package latis.util

import java.io.File
import scala.collection.mutable.ArrayBuffer

object FileUtils {

  /**
   * Get a list of all the files in the given directory and nested directories.
   * Paths with be relative to the given directory.
   */
  def listAllFiles(dir: String): Seq[String] = {  
    
    def accumulateFiles(file: File, buffer: ArrayBuffer[String]) {
      if (file.isDirectory()) file.listFiles().map(accumulateFiles(_, buffer))
      else buffer += file.getPath.drop(dir.length+1)
    }
    
    val buffer = ArrayBuffer[String]()
    accumulateFiles(new File(dir), buffer)
    buffer
  }
  
}