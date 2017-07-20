package latis.reader

import latis.dm._
import latis.data._

import org.junit._
import Assert._
import latis.reader.adapter.Adapter
import latis.data.value.DoubleValue
import latis.data.seq.DataSeq
import latis.metadata.Metadata
import latis.ops.Operation
import latis.reader.tsml.TsmlReader2
import java.net.URL
import latis.ops.filter.FirstFilter

class MockAdapter(m: Model, properties: Map[String, String] = Map.empty) 
  extends Adapter(m, properties) {
  
  val data = DataSeq((0 until 10).map(DoubleValue(_)))
  cache("a", data)
  cache("b", data)
  
  def close {}
}

class TestAdapter {
  
  val model = {
    val a = ScalarType("a", Metadata("type" -> "real"))
    val b = ScalarType("b", Metadata("type" -> "real"))
    val f = FunctionType(a,b, "f", Metadata("length" -> "10"))
    Model(f)
  }
  
  @Test
  def test = {
    val props: Map[String, String] = Map("class" -> "latis.reader.MockAdapter")
    val a = Adapter(model, props)
    //val a = new MockAdapter(model)
    val ops = scala.collection.mutable.ArrayBuffer[Operation]()
    val ds = a.getDataset(ops)
    //latis.writer.Writer.fromSuffix("asc").write(ds)
    assertEquals(10, ds.getLength)
  }
}