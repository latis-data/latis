package latis.ops.filter

import latis.dm.Function
import latis.dm.Sample
import latis.metadata.Metadata
import latis.ops.OperationFactory

class StrideFilter(val stride: Int) extends Filter {
  
  override def applyToFunction(function: Function) = {
    val it = function.iterator.grouped(stride).map(_(0))
    val md = Metadata(function.getMetadata.getProperties + ("length" -> it.length.toString))

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
}
