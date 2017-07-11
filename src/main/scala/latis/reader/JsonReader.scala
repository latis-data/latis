package latis.reader

import java.io.File
import java.net.URL

import scala.io.Source

import latis.dm.Dataset
import latis.dm.Function
import latis.dm.Index
import latis.dm.Sample
import latis.dm.Scalar
import latis.dm.Tuple
import latis.dm.Variable
import latis.metadata.Metadata
import latis.ops.Operation
import latis.time.Time
import latis.util.StringUtils
import play.api.libs.json.JsArray
import play.api.libs.json.JsBoolean
import play.api.libs.json.JsNull
import play.api.libs.json.JsNumber
import play.api.libs.json.JsObject
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
    if (source == null) source = Source.fromURL(StringUtils.getUrl(path))
    source
  }
    
  def close: Unit = {
    if (source != null) source.close
  }
  
  override def getDataset: Dataset = {
  
    //read entire source into string, join with new line
    val jsonString = getDataSource.getLines.mkString(sys.props("line.separator"))
    val value = Json.parse(jsonString)
    val name = path.slice(path.lastIndexOf(File.separator)+1, path.indexOf(".json"))
    
    //wrap the raw json so that it looks like a latis dataset
    val obj = value match {
      case o: JsObject if(o.keys.size == 1) => o //already looks like latis dataset 
      case _ => JsObject(Seq((name, value))) //wrap with dataset name
    }
    
    val v = makeVariable(obj)
    val dsname = obj.keys.head
    Dataset(v, Metadata(dsname))
  }
  
  def getDataset(ops: Seq[Operation]): Dataset = ops.reverse.foldRight(getDataset)(_(_))
  
  /**
   * make a JsValue into a Latis Variable. First tries the value as an array, then an object, and then a scalar
   */
  def makeVariable(obj: JsObject): Variable = {
    val fields = obj.fields
    val v = if(fields.length == 1) fields.head._2 match {
      case a: JsArray => makeFunction(obj)
      case o: JsObject => makeVariable(o)
      case other => makeScalar(obj)
    } else makeTuple(obj)
    v
  }
  
  /**
   * make a Latis Scalar from a JsValue
   */
  def makeScalar(s: JsObject): Scalar = {
    val name = s.keys.head
    s.fields.head._2 match {
      case n: JsNumber => {
        val num = n.value
        if(num.isValidLong) Scalar(Metadata(name), num.longValue)
        else Scalar(Metadata(name), num.doubleValue)
      }
      case t: JsString => if(Time.isValidIso(t.value)) Time.fromIso(t.value)
        else Scalar(Metadata(name), t.value)
      case b: JsBoolean => Scalar(Metadata(name), b.toString)
      case JsNull => Scalar(Metadata(name), "null")
      case _ => ???
    }
  }
  
  /**
   * make a Latis Function from a JsArray
   */
  def makeFunction(arr: JsObject): Function = {
    val name = arr.keys.head
    val it = arr.fields.head._2 match {
      case a: JsArray => a.value
      case _ => ???
    }
    Function(it.map(makeSample(_)), Metadata(name))
  }
  
  def makeSample(value: JsValue): Sample = {
    makeVariable(JsObject(Seq(("sample", value)))) match {
      case t: Tuple => Sample(t.getVariables.head, Tuple(t.getVariables.tail))
      case s => Sample(Index(), s)
    }
  }
  
  /**
   * make Latis Tuple from a JsObject
   */
  def makeTuple(obj: JsObject): Tuple = {
    val name = obj.keys.head
    val vars = obj.fields.map(f => makeVariable(JsObject(Seq(f))))
        
    Tuple(vars, Metadata(name))
  }
}

object JsonReader {
  
  def apply(url: URL): JsonReader = if (url.getPath.contains(".json")) new JsonReader(url.toString) else throw new Exception("JsonReader can only read .json files")

  def apply(path: String): JsonReader = if (path.contains(".json")) new JsonReader(path) else throw new Exception("JsonReader can only read .json files")
  
}
