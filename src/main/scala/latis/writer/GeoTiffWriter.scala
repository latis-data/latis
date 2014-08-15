package latis.writer

import latis.util.FileUtils
import latis.dm.Function
import latis.dm.Dataset
import scala.collection.mutable.ArrayBuilder
import latis.dm.Tuple
import java.awt.image.DataBuffer
import java.awt.image.DataBufferInt
import java.awt.image.Raster
import latis.dm.Sample
import org.geotools.geometry.jts.ReferencedEnvelope
import org.geotools.referencing.CRS
import org.geotools.coverage.grid.GridCoverageFactory
import java.io.File
import java.io.DataOutputStream
import java.io.BufferedOutputStream
import java.io.OutputStream
import java.io.FileOutputStream
import java.awt.image.DataBufferDouble
import java.io.DataInputStream
import java.io.FileInputStream
import latis.util.PeekIterator
import java.awt.image.DataBufferUShort
import java.awt.image.WritableRaster
import org.opengis.geometry.Envelope
import java.awt.image.SinglePixelPackedSampleModel
import java.awt.image.PixelInterleavedSampleModel
import org.geotools.coverage.CoverageFactoryFinder
import org.geotools.factory.Hints

class GeoTiffWriter extends FileWriter {
  
  def makeRaster(function: Function) = {
    val width = function.getMetadata("width").get.toInt
    val height = function.getMetadata("height").get.toInt
    
    val model = new PixelInterleavedSampleModel(DataBuffer.TYPE_INT, width, height, 3, width * 3, Array(0,1,2))
    
    val raster = Raster.createWritableRaster(model, null) //initRaster(width, height, width * height)
    
    val (first, last, len) = fillRaster(function, raster)
    
    (raster, first, last, len)
  }
  
  def fillRaster(function: Function, raster: WritableRaster) = {
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
  
  def makeCoverage(raster: WritableRaster, envelope: Envelope) = {
    val gcf = CoverageFactoryFinder.getGridCoverageFactory(null)
    gcf.create("grid", raster, envelope)
  }
  
  def writeFile(ds: Dataset, file: File) = {
    val function = ds.findFunction.get
    val crs = function.getMetadata("crs").get
    
    val (raster, first, last, len) = makeRaster(function)
    
    val env = new ReferencedEnvelope(first(0), last(0), first(1), last(1), CRS.decode(crs))
    
    val coverage = makeCoverage(raster, env)
    
    val writer = new org.geotools.gce.geotiff.GeoTiffWriter(file)
    writer.write(coverage, null)
    writer.dispose
    coverage.dispose(true)
  }

}