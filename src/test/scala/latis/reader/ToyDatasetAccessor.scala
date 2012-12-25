package latis.reader

import latis.dm._
import scala.collection.immutable._
import latis.writer.AsciiWriter
import scala.util.Random

/**
 * Make a toy Dataset to test the LaTiS API against. 
 */
class ToyDatasetAccessor(val variable: Variable) extends DatasetAccessor {

  val random = new Random(0)
  private[this] var _index: Int = -2
  
  /**
   * Make a Dataset.
   */
  def getDataset() = {
    Dataset(this, variable)
  }
  
  /**
   * Return random value for each Real.
   */
  def getValue(real: Real): Option[Double] = Some(random.nextDouble() * 100)
  
  def getValue(index: Index): Option[Int] = Some(_index)
  
  /**
   * Ten random samples
   */
  def getIterator(function: Function) = new FunctionIterator() {
    
    override def getNextSample() = {
      _index += 1
      if (_index >= 9) null
      else (function.domain, function.range)
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
  
  def function = Function(Index(), real)
  
  
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