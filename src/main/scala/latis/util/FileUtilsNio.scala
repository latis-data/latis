package latis.util

import java.io.File
import java.nio.file.FileVisitResult
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.SimpleFileVisitor
import java.nio.file.attribute.BasicFileAttributes
import scala.collection.mutable.ArrayBuffer
import java.io.InputStream
import java.net.URL
import java.nio.file.FileSystems

/**
 * Utility methods for working with files.
 * 
 * This object is an analog of FileUtils. Specifically, it is where I'm
 * putting file-utility type methods that rely on the nio package in
 * Java7. Since we're still on Java6 for the time being and these
 * methods are just experimental for now, it makes sense to keep them
 * separate. In the future when Java7/nio becomes standard enough, it
 * would be nice to merge these two classes.
 */
object FileUtilsNio {
  
  /**
   * Private inner class used by listAllFilesWithSize
   */
  private class FileSizeAccumulatorVisitor(basePath: String, buffer: ArrayBuffer[String]) extends SimpleFileVisitor[Path] {
    override def visitFile(path: Path, attrs: BasicFileAttributes): FileVisitResult = {
      val file = new File(path.toString())
      // buffer += file.getPath.drop(basePath.length+1) + "," + file.length()
      buffer += getFileNameWithSubdirectory(basePath, file) + "," + file.length()
      return FileVisitResult.CONTINUE;
      
    }
  }
  
  /*
   * Remove the leading basePath and '/' from the file path,
   * or just basePath if there is no '/'. Do not remove first
   * char of file name or subdirectory path.
   */
  def getFileNameWithSubdirectory(basePath: String, file: File): String = {
    if (file.getPath.length > basePath.length && file.getPath.charAt(basePath.length) != File.separatorChar) 
      file.getPath.drop(basePath.length) 
    else 
      file.getPath.drop(basePath.length+1)
  }
  
  /**
   * Analog of FileUtils.listAllFilesWithSize. Returns the same result, but
   * uses Java7's java.nio.file.Files.walkFileTree method internally. In my
   * tests this appears to be about 10% faster than the old method.
   * 
   * Memory usage is still O(n) (populates an entire buffer with the results)
   * so for file systems where you expect n>1m you should use the StreamingFileListAdapter
   * (which is also dependent on Java7's nio package).
   */
  def listAllFilesWithSize(dir: String): Seq[String] = {
    val buffer = ArrayBuffer[String]()
    Files.walkFileTree(Paths.get(dir), new FileSizeAccumulatorVisitor(dir, buffer))
    buffer.sorted
  }
  
  
  /**
   * Download a file and save it locally.
   */
  def downloadFile(url: URL, file: File): Long = {
    //TODO: handle exceptions
    //TODO: create directories?
    var in: InputStream = null
    try {
      in = url.openStream
      Files.copy(in, file.toPath)
    } finally {
      if (in != null) in.close
    }
  }
  
  /**
   * Download a file to a tmp file.
   * Return the name of the file.
   */
  def downloadTmpFile(url: URL): File = {
    val fileName = url.getPath.split(File.separator).last
    val tmpDir = Files.createTempDirectory("latis").toString
    val path = FileSystems.getDefault().getPath(tmpDir, fileName)
    val file = path.toFile
    // TODO: This returns a long.
    downloadFile(url, file)
    file
  }
}
