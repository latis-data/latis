package latis.reader

import java.io._
import latis.util.LatisProperties
import java.io.File
import scala.collection.mutable.ArrayBuffer

class CatalogGenerator { 
  
  private lazy val datasetNames = getListOfDatasets   //turn this into a "catalog": ds of: i -> dsName
  

  
  def getListOfFiles(dir: String): List[File] = { //move this to latis.util.FileUtils
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
      fileNames += x.toString.substring(dsdir.length()+1, x.toString.length-5) //trim leading file path and ".tsml"
    }
    
    fileNames.toList
  }
  
  def getDatasetNames = datasetNames

}