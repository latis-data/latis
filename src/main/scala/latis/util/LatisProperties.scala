package latis.util

import java.util.Properties
import java.io.File
import java.io.FileInputStream
import java.net.URL
import com.typesafe.scalalogging.LazyLogging
import scala.collection.JavaConversions
import java.net.URLDecoder
import java.util.TimeZone

/**
 * Singleton for access to properties with the following precedence:
 * 1) System properties (e.g. so "-Dprop=value" at command line can override)
 * 2) LaTiS properties file
 * 3) Environment variable
 */
class LatisProperties extends Properties with LazyLogging {
  
  /**
   * The name of the properties file.
   */
  val file = getPropertyFileName()

  /**
   * Load the properties when singleton is constructed.
   */
  try {
    logger.debug("Loading properties file: " + file)
    val in = new FileInputStream(file)
    load(in)
    in.close
  } catch {
    case e: Exception => {
      //logger.warn("Unable to load properties file: " + file)
      throw new RuntimeException("Unable to load properties file: " + file, e)
    }
  }

  /**
   * Set the default time zone. Default to UTC.
   * Note, this will only be invoked if LatisProperties is referenced.
   * Server instances will invoke this but some unit tests may not.
   */
  val tz = getProperty("time.zone", "UTC")
  try {
    TimeZone.setDefault(TimeZone.getTimeZone(tz))
  } catch {
    case e: Exception => throw new RuntimeException(s"Unable to set default time zone for: $tz")
  }
  
  /**
   * Find the property file with the following precedence:
   * 1) latis.config system property
   * 2) latis.properties file in LATIS_HOME (environment variable) directory
   * 3) latis.properties file in the classpath
   * latis.properties is traditionally managed in src/main/resources/
   * which gets deployed to the classpath.
   */
  def getPropertyFileName(): String = {
    System.getProperty("latis.config") match { //try system property
      case s: String => s
      case null => System.getenv("LATIS_HOME") match { //try under LATIS_HOME
        case s: String => s + File.separator + "latis.properties"
        case null => getClass.getResource("/latis.properties") match { //try in the classpath
          //Decode URL. Needed for jenkins job. The workspace is the name of the project with spaces.
          //TODO: should we allow loading of properties file from any URL?
          case url: URL => URLDecoder.decode(url.getPath, "UTF-8")
          case null => throw new RuntimeException("Unable to locate property file.")
        }
      }
    }
  }
    
  /**
   * Return the full file system path for the given relative path.
   */
  def resolvePath(path: String): String = {
    //try classpath
    getClass.getResource(File.separator + path) match {
      case url: URL => url.getPath
      //else try the current working directory
      case null => scala.util.Properties.userDir + File.separator + path
    }
  }
  
  /**
   * Override to apply parameter replacement.
   */
  override def getProperty(property: String): String = {
    super.getProperty(property) match {
      case s: String => StringUtils.resolveParameterizedString(s)
      case _ => null
    }
  }
}
  
/**
 * Singleton for access to properties with the following precedence:
 * 1) System properties (e.g. so "-Dprop=value" at command line can override)
 * 2) LaTiS properties file
 * 3) Environment variable
 */
object LatisProperties {
  
  //---- Manage singleton instance ------------------------------------------//
  
  private var _instance: LatisProperties = null
  
  def instance: LatisProperties= {
    if (_instance == null) _instance = new LatisProperties
    _instance
  }
  
  def init(latisProps: LatisProperties) {_instance = latisProps}
  
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
   * Resolve nested properties (in latis.properties) of the form "${prop}".
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
        case _ => System.getenv(javaPropertyNameToEnvVar(property)) match {
          case s: String => Some(s)
          case _ => None
        }
      }
    }
  }
  
  /**
   * Given the name of a java property (i.e. all lower case with "."s)
   * create a conventional Unix environment variable name: all upper case 
   * with "." replaces with "_".
   */
  private def javaPropertyNameToEnvVar(s: String) = s.replace(".", "_").toUpperCase
  
  /**
   * Clear the existing properties causing the singleton to reload the properties.
   */
  def reset {
    //TODO: do we really want to allow this?
    //Used by tests.
     _instance = null
  }
  
  /**
   * Return the known property names.
   * This will only include those in the latis.properties file.
   */
  def keys: Iterable[String] = JavaConversions.dictionaryAsScalaMap(instance).keys.map(_.asInstanceOf[String])

  /**
   * Return the full file system path for the given relative path.
   */
  def resolvePath(path: String): String = instance.resolvePath(path)
  
}