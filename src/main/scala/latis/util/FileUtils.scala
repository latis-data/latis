package latis.util

import java.io.BufferedInputStream
import java.io.File
import java.io.FileInputStream
import java.io.OutputStream

import scala.Array.canBuildFrom
import scala.collection.mutable.ArrayBuffer
import java.net.URL
import java.net.URLDecoder

/**
 * Utility methods for working with files.
 */
object FileUtils {
  
  /**
   * Get the temporary directory (as a File) as defined by the java.io.tmpdir property.
   */
  def getTmpDir: File = new File(System.getProperty("java.io.tmpdir"))
  
  /**
   * Get a unique temporary File.
   */
  def getTmpFile: File = File.createTempFile("latis", null, getTmpDir)
  //TODO: use deleteOnExit?

  def listAllFilesWithSize(dir: String): Seq[String] = {  
    //TODO: performance concern, especially since we are sorting
    //TODO: consider new file io in Java7
    
    def accumulateFiles(file: File, buffer: ArrayBuffer[String]): Unit = {
      if (file.isDirectory()) file.listFiles().foreach(accumulateFiles(_, buffer))
      else buffer += getFileNameWithSubdirectory(dir, file) + "," + file.length
    }
    
    val buffer = ArrayBuffer[String]()
    accumulateFiles(new File(dir), buffer)
    buffer.sorted //sort lexically so the adapter doesn't have to (which it can't if it's Iterative)
  }
  
  /*
   * Remove the leading dir and '/' from the file path,
   * or just dir if there is no '/'. Do not remove first
   * char of file name or subdirectory path.
   */
  def getFileNameWithSubdirectory(dir: String, file: File): String = {
    if (file.getPath.length > dir.length && file.getPath.charAt(dir.length) != File.separatorChar) 
      file.getPath.drop(dir.length) 
    else 
      file.getPath.drop(dir.length+1)
  }
  
  /**
   * Java's File.delete will only remove empty directories.
   */
  def delete(file: File): Unit = {
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
  def streamFile(file: File, out: OutputStream): Unit = {
    //TODO: benchmark
//    val ch = new FileInputStream(fileName).getChannel
//    val mbb = ch.map(FileChannel.MapMode.READ_ONLY, 0L, ch.size)
//    while (mbb.hasRemaining)
//    out.write(mbb.get)
    
    val input = new BufferedInputStream(new FileInputStream(file))
    Iterator.continually(input.read).takeWhile(_ != -1).foreach (out.write)
    out.flush
    input.close
  }
  
  /*
   * Given a directory, return a list of files in that directory
   */
  def getListOfFiles(dir: String): List[File] = { 
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList
    } else {
      List[File]()
    }
  }
  
  /**
   * If this URL is a "file" URL determine if it exists.
   * Decode the URL encoding.
   * Assume "true" for other URL protocols, for now.
   */
  def exists(url: URL): Boolean = {
    url.getProtocol match {
      case "file" => 
        val path = URLDecoder.decode(url.getPath, "UTF-8")
        new File(path).exists
      case _ => true
      //TODO: test http URL by requesting "HEAD"
    }
  }
  
}
