package latis.ops

import latis.dm.Function
import latis.dm.Sample
import latis.dm.Variable
import latis.dm.implicits.variableToDataset
import latis.metadata.Metadata

/**
 * Reduces a Function to a single Sample. The value of Number Variables
 * will be the sum of the value of that Variable in all Samples of the original Function.
 * The value of the domain will be its last value from the original Function.
 * If any Text Variables appear in the range, an UnsupportedOperationException will be thrown. 
 */
class Total extends Operation {
  
  override def applyToFunction(f: Function): Option[Variable] = {
    val addSamples = (s1: Sample, s2: Sample) => Sample(s2.domain, (s1.range + s2.range).unwrap)
    
    val sample = f.iterator.reduceLeft(addSamples)
    val md = Metadata(f.getMetadata.getProperties + ("length"->"1"))
    Some(Function(f.getDomain, f.getRange, Iterator(sample), md))
  }

}

object Total extends OperationFactory{
  override def apply() = new Total
}

