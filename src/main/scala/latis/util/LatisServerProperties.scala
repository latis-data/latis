package latis.util

import java.io.File
import java.net.URL

import javax.servlet.ServletConfig
import java.net.URLDecoder

/**
 * Subclass of LatisProperties that uses the ServletConfig in a server configuration
 * to locate the property file and resolve paths.
 */
class LatisServerProperties(val config: ServletConfig) extends LatisProperties {

  /**
   * Find the property file. Extend parent by looking for a Servlet init parameter
   * (defined in web.xml) which takes precedence.
   */
  override def findPropertyFile: Option[File] = {
    getPropertyFileFrom(config.getInitParameter("config")) orElse
    super.findPropertyFile
  }
      
  /**
   * Return the full file system path for the given relative (to servlet context) path.
   * This will try the classpath first then look relative to the real path of the
   * deployed code.
   */
  override def resolvePath(path: String): String = {
    if (path.startsWith(File.separator)) path
    //try classpath
    else getClass.getResource(File.separator + path) match {
      case url: URL => URLDecoder.decode(url.getPath, "UTF-8")
      //else try the servlet context
      case null => config.getServletContext().getRealPath(path)
    }
  }
}
