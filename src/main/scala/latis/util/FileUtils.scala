package latis.util

import java.io.File
import scala.collection.mutable.ArrayBuffer
import java.io.OutputStream
import java.io.FileInputStream
import java.nio.channels.FileChannel
import java.io.BufferedInputStream

object FileUtils {
  
  def getTmpDir: File = new File(System.getProperty("java.io.tmpdir"))
  
  def getTmpFile: File = ???   //TODO: 
  
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
  
  /**
   * Write a file to the given OutputStream.
   */
  def streamFile(file: File, out: OutputStream) = {
    //TODO: benchmark
//    val ch = new FileInputStream(fileName).getChannel
//    val mbb = ch.map(FileChannel.MapMode.READ_ONLY, 0L, ch.size)
//    while (mbb.hasRemaining)
//    out.write(mbb.get)
    
    val input = new BufferedInputStream(new FileInputStream(file))
    Iterator.continually(input.read).takeWhile (-1 !=).foreach (out.write)
    input.close
  }
}