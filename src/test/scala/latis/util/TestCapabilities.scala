package latis.util

import org.junit._
import Assert._
import java.io.File
import latis.dm._
import latis.metadata._
import latis.util.FileUtils._
import latis.writer.JsonWriter
import latis.metadata.ServerMetadata

class TestCapabilities {
  
  /**
   * Cause the reload of properties before each test.
   */
  @After
  def resetProperties = LatisProperties.reset
  
  @Test
  def capabilities_dataset = {
    //val ds: Dataset = get_capabilities
    val ds = LatisCapabilities().getDataset 
    
    latis.writer.AsciiWriter.write(ds)
    
    assertEquals(true, true) //TODO: write real tests
  }
  
//  /**
//   * Return a Dataset of LaTiS capabilities
//   */
//  def get_capabilities: Dataset = {
//    val datasets: List[Text] = {
//      val dir = LatisProperties.getOrElse("dataset.dir", "src/main/resources/datasets") //TODO: confirm how to make this work in prod and dev
////      getListOfFiles(dir).map(_.getName).map(n => n.substring(0, n.lastIndexOf('.'))) 
//      getListOfFiles(dir).map(f => {
//        val n = f.getName
//        val name = n.substring(0, n.lastIndexOf('.'))
//        val md: Metadata = Metadata().addName("dataset_name")
//        Text(md, name)
//      })
//    }
//    
//    val outputOptions: List[Tuple] = { 
//      ServerMetadata.availableSuffixes.
//        sortBy(suffixInfo => suffixInfo.suffix). //put in alphabetical order
//        map(suffixInfo => (Tuple(Text(suffixInfo.suffix), Text(suffixInfo.description)))). //TODO: breaks if value is null; temporarily changed ServerMetadata to "getOrElse" "" instead of null
//        toList
//    }
//    
//    val filterOptions: List[(Tuple)] = { 
//      ServerMetadata.availableOperations.
//        sortBy(opInfo => opInfo.name). //put in alphabetical order
//        map(opInfo => (Tuple(Text(opInfo.name), Text(opInfo.description), Text(opInfo.usage)))).
//        toList
//    } 
//    
//    val datasetsFunc: Function = Function(datasets)
//    val outputFunc: Function = Function(outputOptions)
//    val filterFunc: Function = Function(filterOptions)
//    
////    datasetsFunc match {
////      case Function(it) => {
////        it.next match {
////          case Sample(i, n) => assertEquals(0.0, i.getNumberData.doubleValue, 0); assertEquals("leap_seconds", n)
////        }
////        it.next match {
////          case Sample(i, n) => assertEquals(1.0, i.getNumberData.doubleValue, 0); assertEquals("properties", n)
////        }
////        it.next match {
////          case Sample(i, n) => assertEquals(2.0, i.getNumberData.doubleValue, 0); assertEquals("", n)
////        }
////      }
////    }
//    
//    val capabilitiesTup: Tuple = Tuple(datasetsFunc, outputFunc, filterFunc)
//    val md = Metadata().addName("latis_capabilities")
//    val ds = Dataset(capabilitiesTup, md)
//    
////    JsonWriter.write(ds)
////    latis.writer.AsciiWriter.write(ds)
////    latis.writer.TextWriter.write(ds)
////    
//    println("Datasets: " + datasets)
//    println("Output Options: " + outputOptions)
//    println("Filter Options: " + filterOptions)
//    
////    val root = "operation"
////    val operationProperties = LatisProperties.getPropertiesWithRoot(root)
////    println(operationProperties)
//   
//    ds
//  }

 
  
}