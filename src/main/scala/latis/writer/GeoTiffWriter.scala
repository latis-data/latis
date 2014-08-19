package latis.writer

import java.awt.image.DataBuffer
import java.awt.image.PixelInterleavedSampleModel
import java.awt.image.Raster
import java.awt.image.WritableRaster
import java.io.File
import org.geotools.coverage.CoverageFactoryFinder
import org.geotools.geometry.jts.ReferencedEnvelope
import org.geotools.referencing.CRS
import org.opengis.geometry.Envelope
import latis.dm.Dataset
import latis.dm.Function
import latis.dm.Tuple
import org.geotools.coverage.grid.GridCoverage2D
import latis.dm.Scalar

class GeoTiffWriter extends FileWriter {
  
  def makeRaster(function: Function): (WritableRaster, Seq[Double], Seq[Double], Int) = {
    val width = function.getMetadata("width").get.toInt
    val height = function.getMetadata("height").get.toInt
    val bands = function.getRange match {
      case _: Scalar => 1
      case t: Tuple => t.getElementCount
    }
    
    val model = new PixelInterleavedSampleModel(DataBuffer.TYPE_BYTE, width, height, bands, width * bands, (0 until bands).toArray)
    
    val raster = Raster.createWritableRaster(model, null)
    
    val (first, last, len) = fillRaster(function, raster)
    
    (raster, first, last, len)
  }
  
  def fillRaster(function: Function, raster: WritableRaster): (Seq[Double], Seq[Double], Int) = {
    val xs = (0 until raster.getWidth).iterator
    val ys = (0 until raster.getHeight)
    val indexes = for(x <- xs; y <- ys) yield (x, y)
    val it = function.iterator.zip(indexes)
    
    val (first, (a,b)) = it.next
    raster.setPixel(a, b, first.getVariables(1).asInstanceOf[Tuple].getVariables.map(_.getNumberData.intValue).toArray)
    var last = first
    var len = 1
    
    for((s, (x, y)) <- it) {
      raster.setPixel(x, y, s.getVariables(1).asInstanceOf[Tuple].getVariables.map(_.getNumberData.intValue).toArray)
      last = s
      len += 1
    }
    
    (first.getVariables(0).asInstanceOf[Tuple].getVariables.map(_.getNumberData.doubleValue),
     last.getVariables(0).asInstanceOf[Tuple].getVariables.map(_.getNumberData.doubleValue), len)
  }
  
  def makeCoverage(raster: WritableRaster, envelope: Envelope): GridCoverage2D = {
    val gcf = CoverageFactoryFinder.getGridCoverageFactory(null)
    gcf.create("grid", raster, envelope)
  }
  
  def writeFile(ds: Dataset, file: File) = {
    val function = ds.findFunction.get
    val crs = function.getMetadata("CRS").get
    
    val (raster, first, last, len) = makeRaster(function)
    
    val env = new ReferencedEnvelope(first(0), last(0), first(1), last(1), CRS.parseWKT(crs))
    val coverage = makeCoverage(raster, env)
    
    val writer = new org.geotools.gce.geotiff.GeoTiffWriter(file)
    writer.write(coverage, null)
    writer.dispose
    coverage.dispose(true)
  }
  
  override def mimeType = "image/tif"

}