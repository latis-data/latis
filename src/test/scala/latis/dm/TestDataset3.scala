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
import latis.metadata.ScalarMetadata

class TestDataset3 {

//  val dataset = {
//    val a = Scalar3()(ScalarMetadata(Map("id" -> "A")))
//    val b = Scalar3()(ScalarMetadata(Map("id" -> "B")))
//    val x = Scalar3()(ScalarMetadata(Map("id" -> "X")))
//    val v = Function3(x, Tuple3(Seq(a,b))())
//    Dataset3("foo", v)
//  }
//  
//  @Test
//  def print = {
//    println(dataset)
//  }
//
//  @Test
//  def toSeq = {
//    dataset.toSeq.foreach(println)
//  }
//
//  @Test
//  def scalars = {
//    dataset.getScalars.foreach(println)
//  }
}