package latis.reader.tsml

import latis.reader.tsml.ml.Tsml
import scala.io.Source
import play.api.libs.json.Json
import latis.ops.Operation
import latis.dm.Dataset

class JsonAdapter(tsml: Tsml) extends TsmlAdapter(tsml) {

  //---- Manage data source ---------------------------------------------------
  
  private var source: Source = null
  
  /**
   * Get the Source from which we will read data.
   */
  def getDataSource: Source = {
    if (source == null) source = Source.fromURL(getUrl)
    source
  }
  
  override def close {
    if (source != null) source.close
  }
  
  
  override def getDataset(ops: Seq[Operation]): Dataset = {
  
    //read entire source into string, join with new line
    val jsonString = getDataSource.getLines.mkString(sys.props("line.separator"))
    val json = Json.parse(jsonString)
    println(json)
    
    null
  }
}