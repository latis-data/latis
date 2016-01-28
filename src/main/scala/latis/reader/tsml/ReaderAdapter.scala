package latis.reader.tsml

import java.net.URL

import latis.ops.Operation
import latis.reader.DatasetAccessor
import latis.reader.tsml.ml.Tsml

/**
 * Access a reader's dataset via an adapter. This class ignores everything defined
 * in the tsml except for the 'location' and 'reader' adapter attributes. 
 */
class ReaderAdapter(tsml: Tsml) extends TsmlAdapter(tsml) {
  
  val reader = {
    val reader_name = getProperty("reader") match {
      case Some(name) => name
      case None => throw new RuntimeException("ReaderAdapter requires a reader class definition 'reader'.")
    }
    try {
      val cls = Class.forName(reader_name)
      val ctor = cls.getConstructor(classOf[URL])
      ctor.newInstance(getUrl).asInstanceOf[DatasetAccessor]
    } catch {
      case e: Exception => {
        throw new Error("Failed to construct Reader: " + reader_name, e)
      }
    }
  }
  
  override def getDataset = reader.getDataset
  
  override def getDataset(ops: Seq[Operation]) = reader.getDataset(ops)
  
  
  def close = reader.close
  
}