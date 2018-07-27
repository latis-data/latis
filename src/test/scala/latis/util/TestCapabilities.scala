package latis.util

import org.junit._
import Assert._
import java.io.File
import latis.util.FileUtils._
import latis.metadata.ServerMetadata

class TestCapabilities {
  
  /**
   * Cause the reload of properties before each test.
   */
  @After
  def resetProperties = LatisProperties.reset
  
  
  @Test
  def get_capabilities = {
    val datasets: List[String] = {
      val dir = LatisProperties.getOrElse("dataset.dir", "src/main/resources/datasets") //TODO: confirm how to make this work in prod and dev
      getListOfFiles(dir).map(_.getName).map(n => n.substring(0, n.lastIndexOf('.'))) 
//      getListOfFiles(dir).map(f => {
//        val n = f.getName
//        n.substring(0, n.lastIndexOf('.'))
//      })
    }
    
    val outputOptions: Seq[(String, String)] = { //Note: Nonexistent descriptions become null
      ServerMetadata.availableSuffixes.
        sortBy(suffixInfo => suffixInfo.suffix). //put in alphabetical order
        map(suffixInfo => (suffixInfo.suffix, suffixInfo.description)).
        toList
    }
    
    val filterOptions: Seq[(String, String, String)] = { //Note: Nonexistent descriptions become null
      ServerMetadata.availableOperations.
        sortBy(opInfo => opInfo.name). //put in alphabetical order
        map(opInfo => (opInfo.name, opInfo.description, opInfo.usage)).
        toList
    } 
    
    println("Datasets: " + datasets)
    println("Output Options: " + outputOptions)
    println("Filter Options: " + filterOptions)
    
//    val root = "operation"
//    val operationProperties = LatisProperties.getPropertiesWithRoot(root)
//    println(operationProperties)
    
    
    assertEquals(true, true)
  }
  
  //@Test
  def capabilities_dataset = {
    val capabilities = ???
    ???
  }
  
//  def getListOfFiles(dir: String): List[String] = {
//    val d = new File(dir)
//    if (d.exists && d.isDirectory) {
//        d.listFiles.filter(_.isFile).toList.map(_.getName)
//    } else {
//        List[File]().map(_.toString) //TODO: an Option could turn this empty list into None; Some(List) and None may be better
//    }
//  }
  
  
  
}