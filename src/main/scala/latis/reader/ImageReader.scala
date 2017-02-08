package latis.reader

import java.io.InputStream
import java.net.URL

import javax.imageio.ImageIO
import latis.dm.Dataset
import latis.dm.Function
import latis.dm.Integer
import latis.dm.Real
import latis.dm.Sample
import latis.dm.Tuple
import latis.metadata.Metadata
import latis.ops.Operation
import latis.util.StringUtils

class ImageReader extends DatasetAccessor {
  //TODO: have readers (DataSource?) manage input stream like Writers manage output, or better
  //  should all readers be constructed with a URL? 
  //  see latis-maven-kp IUVSReader and use with ReaderAdapter in file join in iuvs_kp.tsml
  //TODO: is there a more efficient way to get the pixels? see SampleModel of DataBuffer
  
  private var url: Option[URL] = None
  lazy private val inputStream: InputStream = url match {
    case Some(u) => u.openStream()
    case None => throw new Error("No URL defined for ImageReader.")
  }
  
  def getDataset(operations: Seq[Operation]): Dataset = {
    //(row, col) -> (band0, band1, band2)
    //don't try to re-orient as typical (x,y), use operation for that
    //Raster defines minX and minY to be the upper left.
    val buffer = ImageIO.read(inputStream)
    val raster = buffer.getData()
    val ncol = raster.getWidth
    val nrow = raster.getHeight
    val nbands = raster.getNumBands
    val darray = new Array[Double](nbands) //tmp array to read pixel bands into
    
    val samples = for (row <- 0 until nrow; col <- 0 until ncol) yield {
      val domain = Tuple(Integer(Metadata("row"), row), Integer(Metadata("col"), col))
      val data = raster.getPixel(col, row, darray) //transpose, getPixel wants (x,y)
      val bands = data.zipWithIndex.map(p => Real(Metadata(s"band${p._2}"), p._1))
      val range = Tuple(bands)
      Sample(domain, range)
    }
    
    //add length to function metadata
    val fmd = Metadata("name" -> "image", "nrow" -> nrow.toString(), "ncol" -> ncol.toString())
    val dataset = Dataset(Function(samples, fmd))
    
    //apply operations
    //TODO: apply selections earlier while constructiing samples
    operations.foldLeft(dataset)((ds,op) => op(ds))
  }

  def close {
    try{ if (!url.isEmpty) inputStream.close() } catch {
      case _: Exception => 
    }
  }
}

object ImageReader {
  
  def apply(location: String) = {
    val reader = new ImageReader()
    reader.url = Some(StringUtils.getUrl(location))
    reader
  }
}