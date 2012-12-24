package latis.reader

import latis.dm._
import scala.collection.immutable._
import latis.writer.AsciiWriter

/**
 * Make a toy Dataset to test the LaTiS API against. 
 */
class ToyDatasetAccessor(val variable: Variable) extends DatasetAccessor {

  /**
   * Make a Dataset.
   */
  def getDataset() = {
    Dataset(this, variable)
  }
  
  
  def getValue(real: Real): Option[Double] = Some(scala.util.Random.nextDouble() * 100)
  
  
  def close() {}

}

object ToyDatasetAccessor extends App {
  
  def real = {
    Real()
  }
  
  def realTuple = {
    Tuple(real, real, real)
  }
  
  //make Variable for toy dataset
  //val v = real
  val v = realTuple
  
  //make a DatasetAccessor for this variable
  val da = new ToyDatasetAccessor(v)
  
  //get the Dataset
  val ds = da.getDataset()
  
  //write the Dataset
  val writer = new AsciiWriter(System.out)
  writer.write(ds)
  
}