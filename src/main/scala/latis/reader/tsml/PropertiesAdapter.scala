package latis.reader.tsml

import latis.reader.tsml.ml.Tsml
import latis.data.Data
import latis.data.value.StringValue
import latis.util.LatisProperties
import scala.collection._
import latis.data.DataSeq

/**
 * Use this Adapter and accompanying TSML to expose LaTiS and system properties.
 */
class PropertiesAdapter(tsml: Tsml) extends TsmlAdapter(tsml) {

  override def init = {
//    for (vname <- getOrigScalarNames) {
//      val v = LatisProperties.getOrElse(vname, "")
//      println(vname +": "+v)
//    }
    val dataMap = getOrigScalarNames.map(vname => {
      val pval = LatisProperties.getOrElse(vname, "")
      val sval = StringValue(pval)
      val data = DataSeq(sval)
      (vname, data)
    }).toMap
    
    cache(dataMap)
  }
  
}
