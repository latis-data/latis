package latis.reader.tsml

import scala.io.Source

import latis.data.seq.DataSeq
import latis.dm.Dataset
import latis.reader.tsml.ml.Tsml
import latis.util.StringUtils
import play.api.libs.json.Json

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
  
  lazy val json = Json.parse(getDataSource.mkString)
  
  override def init = {
    getOrigDataset match {
      case Dataset(v) => {
        val vars = v.toSeq
        val names = vars.map(_.getName)
        names.foreach(name => {
          val vals = (json\\name).map(_.toString.stripPrefix("\"").stripSuffix("\""))
          val vtemplate = vars(names.indexOf(name))
          val datas = DataSeq(vals.map(StringUtils.parseStringValue(_, vtemplate)))
          
          cache(name, datas)
        })
      }
    }
    
  }
  
}