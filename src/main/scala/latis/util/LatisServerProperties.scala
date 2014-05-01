package latis.util

import java.io.File
import java.net.URL

import javax.servlet.ServletConfig

/**
 * Subclass of LatisProperties that uses the ServletConfig in a server configuration
 * to locate the property file and resolve paths.
 */
class LatisServerProperties(config: ServletConfig) extends LatisProperties {

  /**
   * Find the property file. Extend parent by looking for a Servlet init parameter
   * (defined in web.xml) which takes precedence.
   */
  override def getPropertyFileName(): String = {
    //Try the init parameters in the web.xml before delegating to super.
    config.getInitParameter("config") match {
      case s: String => s
      case null => super.getPropertyFileName() //delegate to super
    }
  }
      
  /**
   * Return the full file system path for the given relative (to servlet context) path.
   * This will try the classpath first then look relative to the real path of the
   * deployed code.
   */
  override def resolvePath(path: String): String = {
    //try classpath
    getClass.getResource(File.separator + path) match {
      case url: URL => url.getPath
      //else try the servlet context
      case null => config.getServletContext().getRealPath(path)
    }
  }
}
