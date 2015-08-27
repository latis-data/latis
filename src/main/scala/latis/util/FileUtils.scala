package latis.util

import java.io.BufferedInputStream
import java.io.File
import java.io.FileInputStream
import java.io.OutputStream
import scala.Array.canBuildFrom
import scala.collection.mutable.ArrayBuffer
import java.net.URLEncoder
import java.net.URI
import java.net.URL

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
    
    def accumulateFiles(file: File, buffer: ArrayBuffer[String]) {
      if (file.isDirectory()) file.listFiles().foreach(accumulateFiles(_, buffer))
      else buffer += file.getPath.drop(dir.length+1)+","+file.length
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
    Iterator.continually(input.read).takeWhile(_ != -1).foreach (out.write)
    out.flush
    input.close
  }
  
  def getUrl(loc: String) = {
    val uri = new URI(loc)
    if (uri.isAbsolute) uri.toURL //starts with "scheme:...", note this could be file, http, ...
    else if (loc.startsWith(File.separator)) new URL("file:" + loc) //absolute path
    else getClass.getResource("/"+loc) match { //relative path: try looking in the classpath
      case url: URL => url
      case null => {
        
        val dir = scala.util.Properties.userDir
          .split(File.separator)
          .map(URLEncoder.encode(_, "utf-8").replace("+", "%20")) // http://stackoverflow.com/questions/4737841/urlencoder-not-able-to-translate-space-character
          .mkString(File.separator)
          
        new URL("file:" + dir + File.separator + loc) //relative to current working directory
      }
    }
  }
}
