package latis.reader.tsml

import java.awt.geom.Point2D
import latis.reader.tsml.ml.FunctionMl

import java.awt.image.Raster
import java.awt.image.RenderedImage
import scala.Array.canBuildFrom
import org.geotools.coverage.grid.GridCoverage2D
import org.geotools.gce.geotiff.GeoTiffReader
import org.opengis.referencing.operation.MathTransform2D
import latis.data.Data
import latis.reader.tsml.ml.Tsml
import latis.metadata.Metadata
import latis.reader.tsml.ml.VariableMl

class GeoTiffAdapter(tsml: Tsml) extends IterativeAdapter[((Double, Double), Array[Int])](tsml) {
  
  val reader: GeoTiffReader = new GeoTiffReader(getUrl)
  
  val coverage: GridCoverage2D = reader.read(null)
  val transform: MathTransform2D = coverage.getGridGeometry.getGridToCRS2D
  val raster: Raster = coverage.getRenderedImage.getData
  
  val crs = coverage.getCoordinateReferenceSystem2D
  val height = raster.getHeight
  val width = raster.getWidth
  
  def indexes: Iterator[(Int, Int)] = {
    val x = (0 until width).iterator
    val y = (0 until height)
    for(i <- x; j <- y) yield(i, j)
  }
    
  def domain: Iterator[(Double, Double)] = {
    indexes.map(xy => new Point2D.Double(xy._1, xy._2)).map(pt => transform.transform(pt, pt)).map(dom => (dom.getX, dom.getY))
  }
  
  def pixels: Iterator[Array[Int]] = {
    indexes.map(xy => raster.getPixel(xy._1, xy._2, Array.ofDim[Int](raster.getNumBands)))
  }
  
  def close = {
    reader.dispose
  }
  
  override def getRecordIterator: Iterator[((Double, Double), Array[Int])] = {
    domain.zip(pixels)
  }
  
  override def parseRecord(record: ((Double, Double), Array[Int])): Option[Map[String, Data]] = {
    val names = getOrigScalarNames
    val data = recordToData(record)
    
    if(data.length != names.length) None
    else Option(names.zip(data).toMap)
  }
  
  def recordToData(rec: ((Double, Double), Array[Int])): Seq[Data] = {
    List(Data(rec._1._1), Data(rec._1._2)) ++ rec._2.map(i => Data(i))
  }
  
  
  /**
   * Adds metadata from tsml as well as the width, height, and coordinate reference system of the tiff
   */
  override protected def makeMetadata(vml: VariableMl): Metadata = {
    var atts = vml.getAttributes ++ vml.getMetadataAttributes
    
    def addImplicitName(name: String) = {
      if (atts.contains("name")) atts.get("alias") match {
        case Some(a) => atts = atts + ("alias" -> (a+","+name))
        case None => atts = atts + ("alias" -> name)
      } 
      else atts = atts + ("name" -> name)
    }
    if (vml.label == "time") addImplicitName("time")
    if (vml.label == "index") addImplicitName("index")
    
    if(vml.isInstanceOf[FunctionMl]) {
      atts += ("CRS" -> crs.toWKT)
      atts += ("width" -> width.toString)
      atts += ("height" -> height.toString)
    }

    Metadata(atts)
  }
  
}