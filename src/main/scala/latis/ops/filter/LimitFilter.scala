package latis.ops.filter

import latis.dm.Function
import latis.ops.OperationFactory
import latis.dm.Sample
import latis.metadata.Metadata
import latis.dm.WrappedFunction
import latis.util.MappingIterator

/**
 * Keep only the first 'limit' samples of any outer Function in the Dataset.
 */
class LimitFilter(val limit: Int) extends Filter {
  
  override def applyToFunction(function: Function) = {
    //set the new length in the metadata
    //val md = Metadata(function.getMetadata.getProperties + ("length" -> (limit min function.getLength).toString))
    //TODO: 'length' metadata cannot be safely set until getLength does not cause IterableOnce problem
    val it = new MappingIterator(function.iterator.take(limit), (s: Sample) => this.applyToSample(s))
    Some(Function(function.getDomain, function.getRange, it, function.getMetadata))
  }
  
  override def applyToSample(sample: Sample): Option[Sample] = {
    val x = sample.getVariables.map(applyToVariable(_))
    x.find(_.isEmpty) match{
      case Some(_) => None //found an invalid variable, exclude the entire sample
      case None => Some(Sample(x(0).get, x(1).get))
    }
  }
  
}

object LimitFilter extends OperationFactory {
  
  override def apply(args: Seq[String]): LimitFilter = {
    if (args.length > 1) throw new UnsupportedOperationException("The LimitFilter accepts only one argument")
    try {
      LimitFilter(args.head.toInt)
    } catch {
      case e: NumberFormatException => throw new UnsupportedOperationException("The LimitFilter requires an integer argument")
    }
  }
    
  def apply(limit: Int): LimitFilter = new LimitFilter(limit)
}
