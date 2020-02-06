package latis.ops.filter

import latis.dm.Function
import latis.metadata.Metadata
import latis.ops.OperationFactory
import latis.util.LatisServiceException


class TakeOperation(val n: Int) extends Filter {
  override def applyToFunction(function: Function): Option[Function] = {
    //Assume we can hold this all in memory.
    
    //get the first 'n' samples, or all if we had less
    val samples = function.iterator.take(n).toList
    
    //change length of Function in metadata
    val md = function.getMetadata + ("length" -> samples.length.toString)
    
    //make the new function with the updated metadata
    samples.length match {
      case 0 => Some(Function(function.getDomain, function.getRange, Iterator.empty, md)) //empty Function with type of original
      case _ => Some(Function(samples, md))
    }
  }
}

object TakeOperation extends OperationFactory {
  
  override def apply(args: Seq[String]): TakeOperation = {
    if (args.length > 1) throw new LatisServiceException("The TakeOperation accepts only one argument")
    try {
      TakeOperation(args.head.toInt)
    } catch {
      case e: NumberFormatException => throw new LatisServiceException("The TakeOperation requires an integer argument")
    }
  }
    
  def apply(n: Int): TakeOperation = new TakeOperation(n)
  
  def unapply(take: TakeOperation): Option[Int] = Some((take.n))
}