package latis.ops.filter

import latis.dm.Function
import latis.dm.Sample
import latis.metadata.Metadata
import latis.ops.OperationFactory
import latis.util.iterator.MappingIterator


/**
 * Take every "stride-th" element of a Function in the Dataset.
 */
class StrideFilter(val stride: Int) extends Filter {
  
  override def applyToFunction(function: Function) = {
    val it = new MappingIterator(function.iterator.grouped(stride).map(_(0)), (s: Sample) => Some(s))
    val md = function.getMetadata("length") match {
      case None => function.getMetadata
      case Some("0") => function.getMetadata
      case Some(n) => Metadata(function.getMetadata.getProperties + ("length" -> (((n.toInt - 1) / stride) + 1).toString))
    }

    Some(Function(function.getDomain, function.getRange, it, md))
  }
  
}

object StrideFilter extends OperationFactory {
  
  override def apply(args: Seq[String]): StrideFilter = {
    if (args.length > 1) throw new UnsupportedOperationException("The StrideFilter accepts only one argument")
    try {
      StrideFilter(args.head.toInt)
    } catch {
      case e: NumberFormatException => throw new UnsupportedOperationException("The StrideFilter requires an integer argument")
    }
  }
    
  def apply(stride: Int): StrideFilter = new StrideFilter(stride)
  
  def unapply(filter: StrideFilter) = Some(filter.stride)
}
