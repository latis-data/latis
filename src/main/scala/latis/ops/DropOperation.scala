package latis.ops

import latis.dm.Function
import latis.metadata.Metadata
import latis.util.LatisServiceException

class DropOperation(val n: Int) extends Operation {
  override def applyToFunction(function: Function): Option[Function] = {
    //Assume we can hold this all in memory.
    
    //get data without first n samples
    val samples = function.iterator.drop(n).toList
    
    // update length metadata
    val md = function.getMetadata + ("length" -> samples.length.toString)
    
    //make the new function
    samples.length match {
      case 0 => Some(Function(function.getDomain, function.getRange, Iterator.empty, md)) //empty Function with type of original
      case _ => Some(Function(samples))
    }
  }
}

object DropOperation extends OperationFactory {
  
  override def apply(args: Seq[String]): DropOperation = {
    if (args.length > 1) throw new LatisServiceException("The DropOperation accepts only one argument")
    try {
      DropOperation(args.head.toInt)
    } catch {
      case e: NumberFormatException => throw new LatisServiceException("The DropOperation requires an integer argument")
    }
  }
    
  def apply(n: Int): DropOperation = new DropOperation(n)
}