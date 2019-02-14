package latis.util

import java.util.Properties
import java.io.File
import java.io.FileInputStream
import java.net.URL
import com.typesafe.scalalogging.LazyLogging
import scala.collection.JavaConversions
import java.net.URLDecoder
import java.util.TimeZone
import java.io.InputStream
import sbtbuildinfo._

/**
 * Singleton for access to properties with the following precedence:
 * 1) System properties (e.g. so "-Dprop=value" at command line can override)
 * 2) LaTiS properties file
 * 3) Environment variable
 */
class LatisProperties extends Properties with LazyLogging {

  /**
   * Load the properties when singleton is constructed.
   * If the property file is not found, look for latis.properties
   * in the classpath.
   * latis.properties is traditionally managed in src/main/resources/
   * which gets deployed to the classpath.
   */  
  try {
    val in: InputStream = findPropertyFile match {
      case Some(file) => new FileInputStream(file)
      case None => getClass.getResourceAsStream("/latis.properties")
    }
    if (in != null) {
      load(in)
      in.close
    } else throw new RuntimeException("Unable to find latis.properties.")
  } catch {
    case e: Exception => {
      throw new RuntimeException("Unable to load properties file", e)
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
   */
  def findPropertyFile: Option[File] = {
    getPropertyFileFromSystemProperty orElse 
    getPropertyFileFromEnvironmentVar
  }
  
  private def getPropertyFileFromSystemProperty: Option[File] = 
    getPropertyFileFrom(System.getProperty("latis.config"))
  
  private def getPropertyFileFromEnvironmentVar: Option[File] = 
    System.getenv("LATIS_HOME") match {
      case s: String => getPropertyFileFrom(s + File.separator + "latis.properties")
      case _ => None
    }
  
  protected def getPropertyFileFrom(path: String): Option[File] =
    if (path == null) None
    else {
      val file = new File(path)
      if (file.exists) Some(file)
      else None
    }
    
  /**
   * Return the full file system path for the given relative path.
   * If the path is already fully resolved, just return it.
   */
  def resolvePath(path: String): String = {
    if (path.startsWith(File.separator)) path
    //try classpath
    else getClass.getResource(File.separator + path) match {
      case url: URL => URLDecoder.decode(url.getPath, "UTF-8")
      //else try the current working directory
      case null => scala.util.Properties.userDir + File.separator + path
      //TODO: make sure it exists, return Option?
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
   * 1) BuildInfo object (only for the "version" property)
   * 2) System properties (e.g. so "-Dprop=value" at command line can override)
   * 3) LaTiS properties file
   * 4) Environment variable
   */
  def get(property: String): Option[String] = {
    property match {
      case "version" => Some(BuildInfo.version)
      case _ => System.getProperty(property) match {
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
  }
  
  /**
   * Gather the properties that start with a given root (not including the ".").
   * Use the remaining part of the key name for the new property name.
   * This can be used to get all the properties for a given writer, for example,
   * so it doesn't have to know the name that maps to it.
   * e.g. "writer.foo.delimiter" becomes "delimiter"
   */
  def getPropertiesWithRoot(root: String): Map[String, String] = {
    //e.g. "writer.foo"
    val props = scala.collection.mutable.Map[String, String]()
    val keyPrefix = root + "."
    val keys = LatisProperties.keys.filter(_.startsWith(keyPrefix))
    for (key <- keys) {
      val k = key.substring(keyPrefix.length) //skip the root
      props += (k -> LatisProperties(key))
    }
    props.toMap
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
