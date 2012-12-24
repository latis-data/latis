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
  
  /**
   * Return random value for each Real.
   */
  def getValue(real: Real): Option[Double] = Some(scala.util.Random.nextDouble() * 100)
  
  /**
   * Ten random samples
   */
  def getIterator(function: Function) = new FunctionIterator() {
    //val vals = 0 until 10
    private[this] var _index = 0
    println("making iterator")
    
    override def getNextSample() = {
      if (_index >= 10) null
      else {
        _index += 1
        (function.domain, function.range)
      }
    }
    
    //prime the next cache
    _next = getNextSample()
  }
  
  
  def close() {}

}

object ToyDatasetAccessor extends App {
  
  def real = {
    Real()
  }
  
  def realTuple = {
    Tuple(real, real, real)
  }
  
  def function = Function(real, real)
  
  
  //make Variable for toy dataset
  //val v = real
  //val v = realTuple
  val v = function
  
  //make a DatasetAccessor for this variable
  val da = new ToyDatasetAccessor(v)
  
  //get the Dataset
  val ds = da.getDataset()
  
  //write the Dataset
  val writer = new AsciiWriter(System.out)
  writer.write(ds)
  
}