package latis.ops.filter

import latis.dm.Function
import latis.dm.Sample
import latis.metadata.Metadata
import latis.ops.OperationFactory
import latis.util.iterator.MappingIterator
import latis.writer.AsciiWriter
import latis.dm.Dataset

/**
 * Take every "stride-th" element of a Function in the Dataset extended to 2D datasets. 
 */
class Stride2D(val stride1: Int, val stride2: Int) extends Filter {
  
  override def applyToFunction(function: Function) = {
    
    val funList = function.iterator.toList
    var i = 1
    var firstXDone = false
    
    //get the length of the set of X values in the dataset
    val xVal = funList(0).domain.toSeq(0)
    while (i <= funList.length && firstXDone == false) {
      i = i+1
      if (xVal != funList(i).domain.toSeq(0)) {
        firstXDone = true
      }
    }
    
    val stride1Grouped = function.iterator.grouped(i).grouped(stride1)       
    val stride1List = stride1Grouped.toList
    val stride2List = for (z <- (0 until stride1List.size).toList) yield stride1List(z)(0).grouped(stride2)
    var itList = List[Sample]()

    for (j <- 0 until stride2List.size) {
      val listToAdd = new MappingIterator(stride2List(j).map(_(0)), (s: Sample) => Some(s))
      itList = itList ++ listToAdd
    }

    val it = itList.iterator
    //Note: in the original stride filter, there is a length metadata handling method here,
    //but I'm not sure how the metadata for 2D datasets usually looks and how the separate
    //length metadata is handled
    val md = function.getMetadata()
    
//    AsciiWriter.write(Dataset(Function(function.getDomain, function.getRange, it, md)))
    Some(Function(function.getDomain, function.getRange, it, md))
  }
}

object Stride2D extends OperationFactory {
  
  override def apply(args: Seq[String]): Stride2D = {
    if (args.length > 2) throw new UnsupportedOperationException("The Stride2D filter accepts only two arguments")
    else if (args.length < 2) throw new UnsupportedOperationException("Stride2D takes two arguments; use StrideFilter for 1D datasets")
    try {
      Stride2D(args.head.toInt, args.last.toInt)
    } catch {
      case e: NumberFormatException => throw new UnsupportedOperationException("The Stride2D filter requires two integer arguments")
    }
  }
    
  def apply(stride1: Int, stride2: Int): Stride2D = new Stride2D(stride1, stride2)
}
