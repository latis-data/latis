package latis.util

import java.io.File
import scala.collection.mutable.ArrayBuffer

object FileUtils {
  
  /**
   * Get a list of all the files in the given directory and nested directories.
   * Paths with be relative to the given directory.
   */
  def listAllFiles(dir: String): Seq[String] = {  
    //TODO: performance concern, especially since we are sorting
    
    def accumulateFiles(file: File, buffer: ArrayBuffer[String]) {
      if (file.isDirectory()) file.listFiles().map(accumulateFiles(_, buffer))
      else buffer += file.getPath.drop(dir.length+1)
    }
    
    val buffer = ArrayBuffer[String]()
    accumulateFiles(new File(dir), buffer)
    buffer.sorted //sort lexically so the adapter doesn't have to (which it can't if it's Iterative)
  }
  
  /**
   * Java's File.delete will only remove empty directories.
   */
  def delete(file: File) {
    //TODO: deal with IOExceptions, permissions...
    if (file.isDirectory) {
      file.listFiles.map(delete(_))
      file.delete
    }
    else file.delete
  }
  
}