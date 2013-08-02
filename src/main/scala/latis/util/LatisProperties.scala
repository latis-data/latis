package latis.util

import java.util.Properties
import java.io.File
import java.io.FileInputStream
import java.net.URL
import javax.servlet.ServletConfig

class LatisProperties extends Properties {
  //TODO: enforce immutabily? disable setters? extend immutable Map?
  //but might be useful to change properties on the fly?
  //Tempted to use any URL, but could open up complications.

  //Load properties
  val file = getPropertyFileName()
  //Resolve path
  if (file.startsWith(File.separator)) file //already fully resolved
  else resolvePath(file)
  
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
    System.getProperty("latis.config") match { //try system property
      case s: String => s
      case null => System.getenv("LATIS_HOME") match { //try under LATIS_HOME
        case s: String => s + "/latis.properties"
        case null => getClass.getResource("/latis.properties") match { //try in the classpath
          case url: URL => url.getPath
          case null => throw new RuntimeException("Unable to locate property file.")
        }
      }
    }
  }
    
  /**
   * Return the full file system path for the given relative path.
   */
  def resolvePath(path: String): String = {
    scala.util.Properties.userDir + File.separator + path //prepend current working directory
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
  
  //---- Manage singleton instance ------------------------------------------//
  
  private var _instance: LatisProperties = null
  
  def instance: LatisProperties= {
    if (_instance == null) _instance = new LatisProperties
    _instance
  }
  
  def init(config: ServletConfig) {_instance = new LatisServerProperties(config)}
  
  
  //---- Property value access methods --------------------------------------//
  
  /**
   * Directly access the property value. Could be null.
   */
  def apply(property: String): String = get(property) match {
    case Some(s: String) => s
    case _ => null
  }
  
  /**
   * Get the property value or the given default if it does not exist.
   */
  def getOrElse(property: String, default: => String): String = get(property) match {
    case Some(s: String) => s
    case _ => default
  }
  
  /**
   * Get the property as an Option.
   * Order of precedence:
   * 1) System properties (e.g. so "-Dprop=value" at command line can override)
   * 2) LaTiS properties file
   * 3) Environment variable
   */
  def get(property: String): Option[String] = {
    System.getProperty(property) match {
      case s: String => Some(s)
      case _ => instance.getProperty(property) match {
        case s: String => Some(s)
        case _ => System.getenv(property) match {
          case s: String => Some(s)
          case _ => None
        }
      }
    }
  }
  
  
  //----  -------------------------------------------------------------------//
    
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