package latis.reader.tsml.agg

import scala.Option.option2Iterable

import latis.dm.Dataset
import latis.dm.Function
import latis.reader.tsml.TsmlAdapter
import latis.reader.tsml.TsmlReader
import latis.reader.tsml.ml.Tsml

class FileJoinAdapter(tsml: Tsml) extends TileUnionAdapter(tsml) {
  
  override protected val adapters = (tsml.xml \ "dataset").map(n => TsmlAdapter(Tsml(n))).dropRight(1)
  
  val template = (tsml.xml \ "dataset").map(n => Tsml(n)).last
  
  override def collect(datasets: Seq[Dataset]): Dataset = {
    val z = datasets.zip(adapters.map(_.getUrl.toString.replaceAll(" ", "%20")))
    val files = z.flatMap(p => getFileName(p._1, p._2))
    
    val dss = files.map(file => TsmlReader(template.setLocation(file)).getDataset)
    super.collect(dss.toSeq)
  }
  
  def getFileName(ds: Dataset, dir: String) = ds match {
    case Dataset(Function(it)) => it.flatMap(_.toSeq.find(_.hasName("file"))).map(dir + "/" +_.getValue.toString)
  }

}