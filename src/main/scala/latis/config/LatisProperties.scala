package latis.config

import java.util.Properties
import java.io.File
import java.io.FileInputStream
import java.net.URL

class LatisProperties extends Properties {
  //TODO: enforce immutabily? disable setters? extend immutable Map?
  //but might be useful to change properties on the fly?
  //Tempted to use any URL, but could open up complications.

  //Load properties
  val file = getPropertyFileName() //TODO: deal with potential exceptions
  try {
    val in = new FileInputStream(file)
    load(in)
    in.close //TODO: do we need to close the stream?
  } catch {
    case e: Exception => {
      println("[WARN] Unable to load properties file: " + file)
      e.printStackTrace()
    }
  }
            
  /**
   * Find the property file.
   */
  def getPropertyFileName(): String = {
    //Get the property file location
    //TODO: LATIS_HOME env var?
    
    var file = System.getProperty("latis.config") //Try System properties (e.g. command line with "-D"), akin to log4j
    if (file == null) {
      val url = getClass.getResource("/latis.properties") //look in the classpath
      if (url != null) file = url.getPath
    }

    //Prepend absolute path if file is relative.
    resolvePath(file);
  }
    
  /**
   * Return the full file system path for the given relative path.
   */
  def resolvePath(path: String): String = {
    if (path.startsWith(File.separator)) path //already fully resolved
    else scala.util.Properties.userDir + File.separator + path //prepend current working directory
  }
}


/*
 * Just using LatisProperties.getProperty will cause the singleton to be generated.
 * If first invoked with init(config), then the instance will be a LatisServerProperties
 * which will delegate to the ServletConfig for paths and such.
 * TODO: seems like there should be a cleaner way. 
 *   Put if(config!=null) logic here instead of using inheritance polymorphism?
 */
object LatisProperties {
  
  private lazy val instance = new LatisProperties()
  
//  def instance: LatisProperties = {
//    if (_instance == null) _instance = new LatisProperties()
//    _instance
//  }

/*
 * TODO: had to get server stuff out of core, but now the server can't use this
 * only diff is how it finds the properties file
 * 
 */ 
//  //alternate path to create the LatisProperties for the server
//  def init(config: ServletConfig) {
//    _instance = new LatisServerProperties(config)
//  }
  
  //direct access, could be null
  def apply(name: String): String = instance.getProperty(name)
  
  //use the Option monad
  def get(name: String): Option[String] = {
    instance.getProperty(name) match {
      case value: String => Some(value)
      case _ => None
    }
  }
  
  def getOrElse(name: String, default: => String): String = instance.getProperty(name, default)
    
//  //Convenient "static" methods so user doesn't have to get instance first
//  def getProperty(name: String): String = instance.getProperty(name)
//  def getProperty(name: String, default: String ): String = instance.getProperty(name, default)
//
  def resolvePath(path: String): String = instance.resolvePath(path)
  
  
//  /**
//   * Return the full file system pathname of the directory where the default TSML lives.
//   * Defined in latis.properties as "dataset.dir".
//   * Default to the "datasets" directory in the root of the LaTiS web app.
//   */
//   def getDatasetDir() {
//        var datadir = getProperty("dataset.dir", "datasets"); 
//
//        //Prepend absolute path if dataset.dir is relative.
//        if (! datadir.startsWith(File.separator)) {
//            datadir = getRealPath(datadir);
//        }
//        //make sure dir ends with "/"
//        if (! datadir.endsWith(File.separator)) datadir += File.separator;
//        
//        return datadir;
//    }
}