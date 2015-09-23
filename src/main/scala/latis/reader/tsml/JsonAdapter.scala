package latis.reader.tsml

import scala.annotation.migration
import scala.io.Source
import latis.dm.Dataset
import latis.dm.Function
import latis.dm.Sample
import latis.dm.Scalar
import latis.dm.Tuple
import latis.dm.Variable
import latis.metadata.Metadata
import latis.ops.Operation
import latis.reader.tsml.ml.Tsml
import play.api.libs.json.JsArray
import play.api.libs.json.JsNumber
import play.api.libs.json.JsObject
import play.api.libs.json.JsResultException
import play.api.libs.json.JsString
import play.api.libs.json.JsValue
import play.api.libs.json.Json
import play.api.libs.json.JsBoolean
import play.api.libs.json.JsNull
import latis.dm.Index
import java.io.File

class JsonAdapter(tsml: Tsml) extends TsmlAdapter(tsml) {
  
  private var source: Source = null
  
  /**
   * Get the Source from which we will read data.
   */
  def getDataSource: Source = {
    if (source == null) source = Source.fromURL(getUrl)
    source
  }
  
  def close {
    if (source != null) source.close
  }
  
  def path = getUrl.getPath
  
  override def getDataset: Dataset = {
  
    //read entire source into string, join with new line
    val jsonString = getDataSource.getLines.mkString(sys.props("line.separator"))
    //TODO: deal with JsArray at top level
    val fields = Json.parse(jsonString).as[JsObject].fields
    val value: (String, JsValue) = fields.length match {
      case 0 => return(Dataset.empty)
      case 1 => fields(0) //{Dataset: {Variable: ... }}
      case _ => { //{Var1: ... , Var2: ... , ...}
        val name = path.slice(path.lastIndexOf(File.separator)+1, path.indexOf(".json"))
        val vars: JsObject = JsObject(fields)
        (name, JsObject(Seq(("variables", vars))))//{fileName: {variables: {Var1: {...}, Var2: {...}, ...}}}
      }
    }
    val dsname = value._1
    val v = value._2.as[JsObject]
    val vars = getValue(v.fields(0)) //one Variable per Dataset
    //TODO: if more than one field, wrap in Tuple
    
    Dataset(vars, Metadata(dsname))
  }
  
  override def getDataset(ops: Seq[Operation]): Dataset = ops.reverse.foldRight(getDataset)(_(_))
  
  /**
   * make a JsValue into a Latis Variable. First tries the value as an array, then an object, and then a scalar
   */
  def makeValue(name: String, value: JsValue): Variable = {
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
  def getValue(field: (String, JsValue)) = (makeValue _).tupled(field)
  
  /**
   * make a Latis Scalar from a JsValue
   */
  def makeScalar(s: JsValue, name: String): Scalar = {
    try {
      val num = s.as[JsNumber].value
      if(num.isValidLong) Scalar(Metadata(name), num.longValue) 
      else Scalar(Metadata(name), num.doubleValue)
    } catch {
      case jre: JsResultException => try {
        Scalar(Metadata(name), s.as[JsString].value)
      } catch {
        case jre: JsResultException => try {
          Scalar(Metadata(name), s.as[JsBoolean].toString)
        } catch {
          case jre: JsResultException => {
            if(s.equals(JsNull)) Scalar(Metadata(name),"null")
            else ??? //what else could it be???
          }
        }
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
    makeValue("sample", value) match {
      case t: Tuple => Sample(t.getVariables.head, Tuple(t.getVariables.tail))
      case s => Sample(Index(), s)
    }
  }
  
  /**
   * make Latis Tuple from a JsObject
   */
  def makeObject(obj: JsObject, name: String): Tuple = {
    val vars = obj.fields.map(getValue(_))
        
    Tuple(vars, Metadata(name))
  }
}