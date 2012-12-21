package latis.reader

import latis.dm._
import scala.collection.immutable._
import latis.writer.AsciiWriter

/**
 * Make a toy Dataset to test the LaTiS API against. 
 */
class ToyDatasetAccessor extends DatasetAccessor {

  /**
   * Make a Dataset.
   */
  def getDataset() = {
    //
    val v1 = new Real
    val v2 = new Real
    new Dataset(this, Seq(v1, v2))
  }
  
  def close() {}

}

object ToyDatasetAccessor extends App {
  
  //make a DatasetAccessor
  val da = new ToyDatasetAccessor()
  
  //get the Dataset
  val ds = da.getDataset()
  
  //write the Dataset
  val writer = new AsciiWriter(System.out)
  writer.write(ds)
  
}