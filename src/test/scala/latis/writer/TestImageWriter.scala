package latis.writer

import latis.dm._
import java.io.FileOutputStream
import latis.dm.implicits._
import latis.data._
import org.junit._
import Assert._
import latis.writer._
import latis.reader.tsml._
import latis.reader.tsml.ml._
import scala.io.Source
import Assert._
import java.sql._
import latis.reader.tsml.TsmlReader
import scala.collection.mutable.ArrayBuffer
import breeze.plot._
import breeze.linalg._

class TestImageWriter {

  @Test
  def test{
    val ds = TsmlReader("datasets/test/tsi.tsml").getDataset
    Writer("image").write(ds)
  }
  
  //@Test
  def test_breeze{
    val f = Figure()
    val p = f.subplot(0)
    val q = f.subplot(0)
    val x = linspace(0.0,1.0)
    p += plot(x,x:^2.0)
    p.title="p"
    q += plot(x,x:^3.0)
    q.title="q"
    //q.legend=true
    //f.drawPlots()
    f.saveas("src/test/resources/datasets/data/tsi/plot.png")
  }
}