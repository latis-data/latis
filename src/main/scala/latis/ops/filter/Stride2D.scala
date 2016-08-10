package latis.ops.filter

import latis.dm.Function
import latis.dm.Sample
import latis.dm.Tuple
import latis.dm.TupleMatch
import latis.dm.Variable
import latis.metadata.Metadata
import latis.ops.OperationFactory
import latis.util.iterator.MappingIterator
import latis.writer.AsciiWriter
import latis.dm.Dataset


/**
 * Take every "stride-th" element of a Function in the Dataset, extended to
 * 2D data sets. stride1 applies to the x dimension and stride2 applies to
 * the y dimension. This is applicable to Cartesian grids only. 
 * */
class Stride2D(val stride1: Int, val stride2: Int) extends Filter {
  
  override def applyToFunction(function: Function) = {
  
    // First, we find the number of y values for each x value by finding the number of 
    // unique x values and dividing this into the total number of Samples in the function. 
    val funList = function.iterator.toList
    val xNum = funList.map(sample => sample match {
      case Sample(TupleMatch(x,_), _) => x.getData
    }) 
    val yNum = funList.length/xNum.toList.distinct.length

    // These next lines use the given strides to group the data into lists of lists. First,
    // the function iterator is grouped by its y values so that we get a list of lists of Samples 
    // for each x value. Next, we group these lists by the first stride input. This creates a list of lists 
    // that are each the length of the first stride; this is useful because we now only have to consider 
    // the first item in each of these lists. 
    // Next, we group each of the sublists within each first x list into groups the length of stride2. Once 
    // again, we now only consider the first element of each of these lists.  
    val stride1Grouped = function.iterator.grouped(yNum).grouped(stride1)       
    val stride1List = stride1Grouped.toList
    val stride2List = for (z <- (0 until stride1List.size).toList) yield stride1List(z).head.grouped(stride2)
    var finalList = List[Sample]()

    // Here, we find the relevant Samples within our large, confusing list creation.  
    // The initial for loop is to hit each relevant x list. Upon each iteration,
    // a list is created that will be added to the final list. The .map function
    // iterates through each of the lists within the current x list and adds the first 
    // Sample to the current listToAdd. 
    for (j <- 0 until stride2List.size) {
      val listToAdd = new MappingIterator(stride2List(j).map(_.head), (s: Sample) => Some(s))
      finalList = finalList ++ listToAdd
    }

    // Assuming we only have a total length variable in the metadata and not x and y specific lengths, 
    // we correct that here based on the strides. 
    val md = function.getMetadata("length") match {
      case None => function.getMetadata
      case Some("0") => function.getMetadata
      case Some(n) => {
        val mdXNum = ((n.toInt/yNum - 1)/stride1)+1
        val mdYNum = ((yNum - 1)/stride2)+1
        Metadata(function.getMetadata.getProperties + ("length" -> (mdXNum*mdYNum).toString))
      }
    }
    
    //AsciiWriter.write(Dataset(Function(finalList, md)))
    Some(Function(finalList, md))
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
