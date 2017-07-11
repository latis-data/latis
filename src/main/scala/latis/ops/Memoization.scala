package latis.ops

import latis.dm.Function
import latis.dm.Variable

/**
 * Fully realize the Data within the Scalars of the Dataset so it no longer 
 * has ties to the data source and/or can be traversed more than once.
 * Useful for smaller Datasets that we want to read then close the Reader.
 * This will return a new Dataset that is logically equivalent.
 */
class Memoization extends Operation {

  /**
   * Override to realize a TraversableOnce or lazy collection of Samples as a List.
   * This will also add the number of samples to the "length" metadata.
   */
  override def applyToFunction(function: Function): Option[Variable] = {
    val md = function.getMetadata
    val samples = function.iterator.toList.flatMap(applyToSample(_))
    
    //Add 'length' to the Function Metadata
    val md2 = md + ("length" -> samples.length.toString)
 
    samples.length match {
      case 0 => Some(Function(function.getDomain, function.getRange, md2))
      case _ => Some(Function(samples, md2))
    }
    
  }
}

object Memoization {
  def apply(): Memoization = new Memoization()
}