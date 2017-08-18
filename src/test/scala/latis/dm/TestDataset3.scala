package latis.dm

import latis.dm.implicits._
import org.junit._
import Assert._
import com.typesafe.scalalogging.LazyLogging
import latis.dm._
import latis.metadata.Metadata
import latis.time.Time
import latis.writer.AsciiWriter
import java.nio.ByteBuffer
import latis.data.value.DoubleValue
import latis.reader.tsml.TsmlReader

class TestDataset3 {

  val dataset = {
    val a = Scalar3("A")
    val b = Scalar3("B")
    val x = Scalar3("X")
    val v = Function3("", Metadata.empty, x, Tuple3(a,b))
    Dataset3("foo", v)
  }
  
  @Test
  def print = {
    println(dataset)
  }

  @Test
  def toSeq = {
    dataset.toSeq.foreach(println)
  }

  @Test
  def scalars = {
    dataset.getScalars.foreach(println)
  }
}