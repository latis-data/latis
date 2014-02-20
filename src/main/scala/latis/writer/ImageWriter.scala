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

class ImageWriter extends Writer{
  
  val f = Figure()
  
  def write(dataset: Dataset) {
    plotData(dataset)
    val name=dataset.getName
    f.saveas("src/test/resources/datasets/data/tsi/plot.png")
  }
  
  def plotData(dataset: Dataset, style: String = "plot") {
    val function = dataset.findFunction.get
    val a = function.getDomain
    val b = function.getRange
    b match{
      case _:Tuple => for(c <- b.toSeq) plotFunction(a.getName,c.getName,dataset)
      case _:Scalar => plotFunction(a.getName,b.getName,dataset)
    }
  }
  
  def plotFunction(a: String, b: String, dataset: Dataset, style: Char = '-') {
    val data = latis.util.DataMap.toDoubleMap(dataset)
    val x = data(a)
    val y = data(b)
    val p = f.subplot(0)
    p += plot(x,y)
    p.xlabel = a
    p.ylabel = b
    p.title = dataset.getName
  }

}