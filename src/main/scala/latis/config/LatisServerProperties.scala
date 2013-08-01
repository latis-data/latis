package latis.config

import javax.servlet.ServletConfig
import java.net.URL

class LatisServerProperties(config: ServletConfig) extends LatisProperties {

  /**
   * Find the property file. Extend parent by looking for a Servlet init parameter.
   */
  override def getPropertyFileName(): String = {
    //Try the init parameters in the web.xml
    var path = config.getInitParameter("config")
    
    if (path == null) super.getPropertyFileName() //delegate to super
    else path
  }
      
  /**
   * Return the full file system path for the given relative (to servlet context) path.
   */
  override def resolvePath(path: String): String = {
    config.getServletContext().getRealPath(path)
  }
}
