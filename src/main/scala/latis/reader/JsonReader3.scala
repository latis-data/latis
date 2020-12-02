package latis.reader

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
import java.io.InputStream

// Experimental replacement for JsonReader in latis core which
// does not capture the key of objects which is needed as the tuple name

/**
 * Creates a Dataset from a json file. Objects are interpreted as Tuples, Arrays are interpreted as Functions.
 */
class JsonReader3(source: Source) extends DatasetAccessor {

  def close: Unit = source.close

  override def getDataset: Dataset = {

    //read entire source into string, join with new line
    val jsonString = source.getLines.mkString(sys.props("line.separator"))
    val value: JsValue = Json.parse(jsonString)
    val name = "anonymous" //path.slice(path.lastIndexOf(File.separator)+1, path.indexOf(".json"))

    //    //wrap the raw json so that it looks like a latis dataset
    //    val obj = value match {
    //      case o: JsObject if(o.keys.size == 1) => o //already looks like latis dataset
    //      case _ => JsObject(Seq((name, value))) //wrap with dataset name
    //    }

    val v = makeVariable("", value)
    Dataset(v, Metadata(name))
  }

  def getDataset(ops: Seq[Operation]): Dataset = ops.reverse.foldRight(getDataset)(_(_))

  /**
   * make a JsValue into a Latis Variable. First tries the value as an array, then an object, and then a scalar
   */
  def makeVariable(name: String, jsval: JsValue): Variable = jsval match {
    case a: JsArray => makeFunction(name, a)
    case o: JsObject => makeTuple(name, o)
    case _ => makeScalar(name, jsval)
    //    val fields = obj.fields
    //    val v = if(fields.length == 1) fields.head._2 match {
    //      case a: JsArray => makeFunction(obj)
    //      case o: JsObject => makeVariable(o)
    //      case other => makeScalar(obj)
    //    } else makeTuple(obj)
    //    v
  }

  /**
   * make a Latis Scalar from a JsValue
   */
  def makeScalar(name: String, jsval: JsValue): Scalar = {
    jsval match {
      case n: JsNumber => {
        val num = n.value
        if(num.isValidLong) Scalar(Metadata(name), num.longValue)
        else Scalar(Metadata(name), num.doubleValue)
      }
      case t: JsString => if(Time.isValidIso(t.value)) Time.fromIso(t.value) //TODO: Time as Text with orig name
      else Scalar(Metadata(name), t.value)
      case b: JsBoolean => Scalar(Metadata(name), b.toString)
      case JsNull => Scalar(Metadata(name), "null")
      case _ => ???
    }
  }

  /**
   * make a Latis Function from a JsArray
   */
  def makeFunction(name: String, arr: JsArray): Function = {
    val samples = arr.value.map(makeSample(_))
    Function(samples, Metadata(name))
  }

  def makeSample(jsval: JsValue): Sample = {
    Sample(Index(), makeVariable("", jsval))
    //    makeVariable(JsObject(Seq(("sample", value)))) match {
    //      case t: Tuple => Sample(t.getVariables.head, Tuple(t.getVariables.tail))
    //      case s => Sample(Index(), s)
    //    }
  }

  /**
   * make Latis Tuple from a JsObject
   */
  def makeTuple(name: String, obj: JsObject): Tuple = {
    //val vars = obj.fields.map((n,v) => makeVariable(n,v))
    val vars = obj.fields.map(p => makeVariable(p._1, p._2))
    Tuple(vars, Metadata(name))
  }
}

object JsonReader3 {

  def apply(source: Source): JsonReader3 =
    new JsonReader3(source)

  def apply(inputStream: InputStream): JsonReader3 =
    new JsonReader3(Source.fromInputStream(inputStream))

  def apply(url: URL): JsonReader3 = JsonReader3(url.toString)

  def apply(path: String): JsonReader3 =
    new JsonReader3(Source.fromURL(StringUtils.getUrl(path)))

}
