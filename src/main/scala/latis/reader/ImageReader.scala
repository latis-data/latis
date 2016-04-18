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
    val buffer = ImageIO.read(inputStream)
    val raster = buffer.getData()
    val nx = raster.getWidth
    val ny = raster.getHeight
    val nbands = raster.getNumBands
    val darray = new Array[Double](nbands) //tmp array to read pixel bands into
    
    val samples = for (x <- 0 until nx; y <- 0 until ny) yield {
      val domain = Tuple(Integer(Metadata("x"), x), Integer(Metadata("y"), y))
      val data = raster.getPixel(x, ny-y-1, darray) //reverse y-direction
      val bands = data.zipWithIndex.map(p => Real(Metadata(s"band${p._2}"), p._1))
      val range = Tuple(bands)
      Sample(domain, range)
    }
    
    //add length to function metadata
    val fmd = Metadata("nx" -> nx.toString(), "ny" -> ny.toString())
    val dataset = Dataset(Function(samples, fmd))
    
    //apply operations
    //TODO: apply selections earlier while constructiing samples
    operations.foldLeft(dataset)((ds,op) => op(ds))
  }

  def close {
    if (url != null) inputStream.close()
  }
}

object ImageReader {
  
  def apply(url: String) = {
    val reader = new ImageReader()
    reader.url = Some(new URL(url))
    reader
  }
}