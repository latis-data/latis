package latis.reader

import java.io.File
import java.net.URI
import java.net.URL

import scala.io.Source

import latis.dm.Dataset
import latis.dm.Function
import latis.dm.Sample
import latis.dm.Scalar
import latis.dm.Tuple
import latis.dm.Variable
import latis.metadata.Metadata
import latis.ops.Operation
import play.api.libs.json.JsArray
import play.api.libs.json.JsNumber
import play.api.libs.json.JsObject
import play.api.libs.json.JsResultException
import play.api.libs.json.JsString
import play.api.libs.json.JsValue
import play.api.libs.json.Json

/**
 * Creates a Dataset from a json file. Objects are interpreted as Tuples, Arrays are interpreted as Functions.
 */
class JsonReader(path: String) extends DatasetAccessor {
  
  private var source: Source = null
  
  /**
   * Get the Source from which we will read data.
   */
  def getDataSource: Source = {
    if (source == null) source = Source.fromURL(getUrl)
    source
  }
  
  def getUrl: URL = {
    val uri = new URI(path)
    if (uri.isAbsolute) uri.toURL //starts with "scheme:...", note this could be file, http, ...
    else if (path.startsWith(File.separator)) new URL("file:" + path) //absolute path
    else getClass.getResource("/"+path) match { //relative path: try looking in the classpath
      case url: URL => url
      case null => new URL("file:" + scala.util.Properties.userDir + File.separator + path) //relative to current working directory
    }
  }
  
  def close {
    if (source != null) source.close
  }
  
  def getDataset: Dataset = {
  
    //read entire source into string, join with new line
    val jsonString = getDataSource.getLines.mkString(sys.props("line.separator"))
    val value = Json.parse(jsonString).as[JsObject].value.toSeq
    val name = value(0)._1
    val vars = value(0)._2.as[JsObject].fields.map(a => makeValue(a._2, a._1))
    
    val v = vars.length match {
      case 0 => ???
      case 1 => vars.head
      case _ => Tuple(vars) //wrap multiplevars in a Tuple
    }
    
    Dataset(v, Metadata(name))
  }
  
  /**
   * make a JsValue into a Latis Variable. First tries the value as an array, then an object, and then a scalar
   */
  def makeValue(value: JsValue, name: String): Variable = {
    try {
      val array = value.as[JsArray]
      makeArray(array, name)
    } catch {
      case jre: JsResultException => try {
        val obj = value.as[JsObject]
        makeObject(obj, name)
      } catch {
        case jre: JsResultException => makeScalar(value, name)
      }
    }
  }
  
  /**
   * make a Latis Scalar from a JsValue
   */
  def makeScalar(s: JsValue, name: String): Scalar = {
    try {
      val num = s.as[JsNumber].value
      if(num.isValidLong) Scalar(Metadata(name), num.longValue) else Scalar(Metadata(name), num.doubleValue)
    } catch {
      case jre: JsResultException => try {
        Scalar(Metadata(name), s.as[JsString].value)
      } catch {
        case jre: JsResultException => ??? //I think all scalars will be either numbers or strings
      }
    }
  }
  
  /**
   * make a Latis Function from a JsArray
   */
  def makeArray(arr: JsArray, name: String): Function = {
    val it = arr.value
    Function(it.map(makeSample(_)), Metadata(name))
  }
  
  def makeSample(value: JsValue): Sample = {
    try {
      val obj = value.as[JsObject]
      val vars = obj.fields.map(a => makeValue(a._2, a._1))
      Sample(vars.head, Tuple(vars.tail))
    } catch { case jre: JsResultException => ??? }//this would indicate an index
  }
  
  /**
   * make Latis Tuple from a JsObject
   */
  def makeObject(obj: JsObject, name: String): Tuple = {
    val vars = obj.fields.map(a => makeValue(a._2, a._1))
        
    Tuple(vars, Metadata(name))
  }
}

object JsonReader {
  
  def apply(url: URL) = if (url.getPath.endsWith(".json")) new JsonReader(url.getPath) else throw new Exception("JsonReader can only read .json files")

  def apply(path: String) = if (path.endsWith(".json")) new JsonReader(path) else throw new Exception("JsonReader can only read .json files")
  
}