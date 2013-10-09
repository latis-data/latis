package latis.util

import java.io.File
import scala.collection.mutable.ArrayBuffer

object FileUtils {

  def listAllFiles(dir: String): Seq[String] = {  
    def accumulateFiles(file: File, buffer: ArrayBuffer[String]) {
      if (file.isDirectory()) file.listFiles().map(accumulateFiles(_, buffer))
      else buffer += file.getAbsolutePath() //TODO: canonical?
    }
    
    val buffer = ArrayBuffer[String]()
    accumulateFiles(new File(dir), buffer)
    buffer
  }
  
}