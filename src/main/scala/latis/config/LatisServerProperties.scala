package latis.config

import javax.servlet.ServletConfig
import java.net.URL

//TODO: not currently being used due to how we are doing the latisProperties singleton, keeping server out of the core

class LatisServerProperties(config: ServletConfig) extends LatisProperties {

  /**
   * Find the property file.
   */
  override def getPropertyFileName(): String = {
    //Try the init parameters in the web.xml
    var path = config.getInitParameter("config")
    
    if (path == null) super.getPropertyFileName() //delegate to super
    else resolvePath(path) //need to get full path, super already does this
  }
      
  /**
   * Return the full file system path for the given relative (to servlet context) path.
   */
  override def resolvePath(path: String): String = {
    config.getServletContext().getRealPath(path)
  }
}
