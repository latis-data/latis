package latis.reader

import latis.dm._
import scala.collection.immutable._
import latis.writer.AsciiWriter
import scala.util.Random

/**
 * Make a toy Dataset to test the LaTiS API against. 
 */
class ToyDatasetAccessor() extends DatasetAccessor {
  
  //random number generator with a fixed seed so we get the same results each time
  val random = new Random(0) 
  
  /**
   * Make a Dataset.
   */
  def getDataset() = {
    //val v = Real(3.14)
    //val v = Tuple(Integer(1), Real(1), Text("1"))
    //val v = index_function
    //val v = random_function(10)
    //val v = function_from_iterators
    val v = nested_function
    
    Dataset(this, v)
  }
  
  
  def index_function = Function(IndexSet(10), (1 to 10).map(Real(_)) )
  
  def random_function(length: Int) = {
    Function(IndexSet(length), (1 to length).map(x => Real(random.nextDouble())) )
  }
  
  def function_from_iterators = {
    val domain = DomainSet((0 until 10).map(Index(_)).iterator)
    val range = VariableSeq((1 to 10).map(Real(_)).iterator)
    Function(domain, range)
  }
  
  def nested_function = {
    val domain = IndexSet(3)
    val range = Seq(random_function(3), random_function(3), random_function(3))
    Function(domain, range)
  }

  
  def close() {}

}

object ToyDatasetAccessor extends App {

  val da = new ToyDatasetAccessor()
  val ds = da.getDataset()
  val writer = new AsciiWriter(System.out)
  writer.write(ds)
  
}
