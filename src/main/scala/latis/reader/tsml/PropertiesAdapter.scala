package latis.reader.tsml

import latis.reader.tsml.ml.Tsml
import latis.data.Data
import latis.data.value.StringValue
import latis.util.LatisProperties
import scala.collection._

/**
 * Use this Adapter and accompanying TSML to expose LaTiS and system properties.
 */
class PropertiesAdapter(tsml: Tsml) extends GranuleAdapter(tsml) {

  def readData: Map[String, Data] = {
//    for (vname <- getOrigScalarNames) {
//      val v = LatisProperties.getOrElse(vname, "")
//      println(vname +": "+v)
//    }
    getOrigScalarNames.map(vname => (vname, StringValue(LatisProperties.getOrElse(vname, "")))).toMap
  }
  
}
