package latis.writer

import latis.dm._
import scala.io.Source
import breeze.plot._
import breeze.linalg._
import latis.writer._
import latis.reader.tsml._
import latis.reader.tsml.ml._
import org.junit._
import Assert._
import java.sql._
import latis.dm._
import latis.dm.implicits._
import latis.data._
import org.junit._
import Assert._
import latis.writer._
import latis.reader.tsml._
import latis.reader.tsml.ml._
import latis.ops.Projection
import scala.collection.mutable.ArrayBuffer
import latis.ops.Operation
import latis.ops.Selection
import latis.ops.LastFilter
import latis.ops.FirstFilter
import latis.ops.LimitFilter
import scala.io.Source
import java.sql._
import latis.reader.tsml.TsmlReader
import scala.collection.mutable.ArrayBuffer
import latis.ops._

class ImageWriter {
  
  @Test
  def test_plot{
    val ds = TsmlReader("datasets/tsi.tsml").getDataset
    val data = latis.util.DataMap.toDoubleMap(ds)
    val a = data("year")
    val b = data("tsi")
    
    val f = Figure()
    val p = f.subplot(0)
    val x = linspace(0.0,1.0)
    p+=plot(a,b)
    p.title = "tsi"
    p.xlabel = "year"
    p.ylabel = "tsi"
    f.saveas("src/test/resources/datasets/data/tsi/plot.png")
  }
  
}