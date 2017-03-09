package latis.writer

import latis.dm.Dataset
import latis.dm.Binary
import latis.dm.Dataset
import latis.dm.Function
import latis.dm.Index
import latis.dm.Integer
import latis.dm.Real
import latis.dm.Sample
import latis.dm.Scalar
import latis.dm.Text
import latis.dm.Tuple
import latis.dm.Variable
import java.io.PrintWriter
import java.io._
import latis.dm.TupleMatch
import scala.annotation.meta.companionObject
import latis.util.LatisProperties
import java.net.URL
import java.io.File
import scala.collection.mutable.ArrayBuffer

class CatalogGenerator extends Writer {
  
  private lazy val datasetNames = getListOfDatasets  
  
  def write(dataset: Dataset) {
    ???
  }
  
  def getListOfFiles(dir: String): List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList
    } else {
      List[File]()
    }
  }
  
  def getListOfDatasets: List[String] = {
    val dsdir: String = LatisProperties.getOrElse("dataset.dir", "datasets")
    val fileList = getListOfFiles(dsdir) 
    val fileNames = ArrayBuffer[String]()
    
    for (x <- fileList) {
      fileNames += x.toString.substring(dsdir.length()+1)
    }
    
    fileNames.toList
  }
  
  def getDatasetNames = datasetNames

}