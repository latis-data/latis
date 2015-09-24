package latis.ops

import latis.dm.Function
import latis.metadata.Metadata


class TakeOperation(val n: Int) extends Operation with Idempotence {
  override def applyToFunction(function: Function) = {
    //Assume we can hold this all in memory.
    
    //get the first 'limit' samples, or all if we had less
    val samples = function.iterator.take(n).toList
    
    //change length of Function in metadata
    val md = Metadata(function.getMetadata.getProperties + ("length" -> samples.length.toString))
    
    //make the new function with the updated metadata
    samples.length match {
      case 0 => Some(Function(function.getDomain, function.getRange, Iterator.empty, md)) //empty Function with type of original
      case _ => Some(Function(samples, md))
    }
  }
  
}

object TakeOperation extends OperationFactory {
  
  override def apply(args: Seq[String]): TakeOperation = {
    if (args.length > 1) throw new UnsupportedOperationException("The TakeOperation accepts only one argument")
    try {
      TakeOperation(args.head.toInt)
    } catch {
      case e: NumberFormatException => throw new UnsupportedOperationException("The TakeOperation requires an integer argument")
    }
  }
    
  def apply(n: Int): TakeOperation = new TakeOperation(n)
}