package latis.ops.filter

import latis.dm.Function
import latis.dm.Sample
import latis.metadata.Metadata
import latis.ops.OperationFactory
import latis.util.LatisServiceException

import scala.collection.mutable.ListBuffer

class TakeRightOperation(val n: Int) extends Filter {

  override def applyToFunction(function: Function): Option[Function] = {
    //Assume we can hold this all in memory.

    (n, function) match {
      case (_, Function(it)) if it.isEmpty => Some(Function(function.getDomain, function.getRange, Iterator.empty, function.getMetadata()))
      case (i: Int, _) if (i <= 0) => Some(Function(function.getDomain, function.getRange, Iterator.empty, function.getMetadata()))
      case (i: Int, _) => {
          val it = function.iterator
          val buffer = ListBuffer[Sample]()
          it.foreach { record =>
            if (buffer.length >= n) {
              buffer.remove(0)
            }
              buffer.append(record)
          }

          val samples = buffer.toList
            
          //change length of Function in metadata
          val md = function.getMetadata + ("length" -> samples.length.toString)
          //make the new function with the updated metadata
          samples.length match {
            case 0 => Some(Function(function.getDomain, function.getRange, Iterator.empty, md)) //empty Function with type of original
            case _ => Some(Function(samples, md))
          }
      }
    }
  } 
}

object TakeRightOperation extends OperationFactory {
  
  override def apply(args: Seq[String]): TakeRightOperation = {
    if (args.length > 1) throw new LatisServiceException("The TakeRightOperation accepts only one argument")
    try {
      TakeRightOperation(args.head.toInt)
    } catch {
      case e: NumberFormatException => throw new LatisServiceException("The TakeRightOperation requires an integer argument")
    }
  }
    
  def apply(n: Int): TakeRightOperation = new TakeRightOperation(n)
  
  def unapply(takeRight: TakeRightOperation): Option[Int] = Some((takeRight.n))
}