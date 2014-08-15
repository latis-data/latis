package latis.reader.tsml

import java.io.File
import java.io.BufferedInputStream
import java.io.FileInputStream
import java.nio.ByteBuffer
import java.nio.ByteOrder
import latis.dm.Integer
import latis.dm.Real
import latis.metadata.Metadata
import latis.dm.Dataset
import latis.dm.Tuple
import latis.reader.tsml.ml.Tsml
import scala.collection.mutable.ListBuffer
import latis.data.Data
import java.io.DataInputStream
import javax.imageio.stream.FileImageInputStream
import org.geotools.gce.geotiff.GeoTiffReader
import org.geotools.coverage.grid.GridCoverage2D
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.ArrayBuilder
import java.awt.geom.Point2D

class GeoTiffAdapter(tsml: Tsml) extends IterativeAdapter[((Double, Double), Array[Int])](tsml) {
  
  val reader = new GeoTiffReader(getUrl)
  
  val coverage: GridCoverage2D = reader.read(null)
  lazy val crs = coverage.getCoordinateReferenceSystem2D
  val image = coverage.getRenderedImage
  val transform = coverage.getGridGeometry.getGridToCRS2D
  
  val raster = image.getData
  
  val height = raster.getHeight
  val width = raster.getWidth
  
  def indexes = {
    val x = (0 until width).iterator
    val y = (0 until height)
    for(i <- x; j <- y) yield(i, j)
  }
  
  def getIndexes = indexes.duplicate
  
  def domain = {
    indexes.map(xy => new Point2D.Double(xy._1, xy._2)).map(pt => transform.transform(pt, pt)).map(dom => (dom.getX, dom.getY))
  }
  
  def pixels = {
    indexes.map(xy => raster.getPixel(xy._1, xy._2, Array.ofDim[Int](raster.getNumBands)))
  }
  
  def close = {
    reader.dispose
  }
  
  override def getRecordIterator: Iterator[((Double, Double), Array[Int])] = {
//    println(crs.toWKT)
    domain.zip(pixels)
  }
  
  override def parseRecord(record: ((Double, Double), Array[Int])): Option[Map[String, Data]] = {
    val names = getOrigScalarNames
    val data = recordToData(record)
    
    if(data.length != names.length) None
    else Option(names.zip(data).toMap)
  }
  
  def recordToData(rec: ((Double, Double), Array[Int])) = {
    List(Data(rec._1._1), Data(rec._1._2)) ++ rec._2.map(i => Data(i))
  }
  
}