package latis.reader

import latis.dm._
import scala.collection.immutable._
import latis.writer.AsciiWriter
import scala.util.Random

/**
 * Make a toy Dataset to test the LaTiS API against. 
 */
class ToyDatasetAccessor(val variable: Variable) extends DatasetAccessor {

  //random number generator with a fixed seed so we get the same results each time
  val random = new Random(0) 
  
  //keep count of what sample we are on, by the nature of the algorithm we need to start early
  private[this] var _index: Int = -2
  
  /**
   * Make a Dataset around the Variable we were constructed with.
   */
  def getDataset() = {
    Dataset(this, variable)
  }
  
  /**
   * Make up data:
   *   random value for each Real
   *   int from 0 until 10 for Index
   */
  def getValue(v: Scalar[_]): Option[_] = v match {
    case r: Real => Some(random.nextDouble() * 100)
    case i: Index => Some(_index)
  }
  
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