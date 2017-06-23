package latis.ops

import scala.collection.mutable.ArrayBuffer

import latis.data.Data
import latis.dm.Function
import latis.dm.Sample
import latis.dm.Scalar
import latis.dm.Tuple
import latis.dm.Dataset
import latis.dm.implicits.variableToDataset
import latis.util.iterator.PeekIterator

/**
 * Takes a function with a Tuple domain with a finite number 
 * of allowed values for one variable and reshapes the domain into 
 * a tuple indexed by those allowed values. 
 * 
 * Written primarily for the timed_see_xps_diodes_l2(a) datasets in Lisird, 
 * may need some work for other cases.
 */
class Pivot(vname: String, l: Int) extends Operation {
  
  override def applyToFunction(f: Function): Option[Function] = {
    val pit = PeekIterator(f.iterator)
    
    val nit = PeekIterator( new Iterator[Sample] {
      override def hasNext = pit.hasNext
      
      override def next = {
        val buf = ArrayBuffer[Sample]()
        buf += pit.next
        //group all samples with the same time
        while(pit.hasNext && pit.peek.domain == buf.head.domain) {
          buf += pit.next
        }
        
        Sample(buf.head.domain, combineVars(buf))
      }
      
      //combine the set of samples with the same time to create a pivoted tuple
      def combineVars(samples: Seq[Sample]) = {
        //get pairs of variables with their pivot numbers
        //only pivots on the first scalar after the pivot variable
        val pivots: Seq[(Int, Scalar)] = samples.map(_ match {
          case Sample(_, Tuple(Seq(s: Scalar, v: Scalar))) if(s.hasName(vname)) => (s.getNumberData.intValue, v)
          case Sample(_, Tuple(Seq(s: Scalar, v: Scalar, _))) if(s.hasName(vname)) => (s.getNumberData.intValue, v)
        })
        
        val vtemp = pivots.head._2       
        val missingData = Data(pivots.head._2.getMissingValue)
        val renamedVar = (i: Int) => RenameOperation(vtemp.getName, vname+i)(vtemp) match {
          case Dataset(v) => v
          case _ => null
        }
        //for a pivot index, see if we have data for that index or else return missing data
        val getData = (i: Int) => pivots.find(_._1 == i) match {
          case Some((_, v)) => v.getData
          case None => missingData
        }
        //tabulate the pivoted variables with a new name and the appropriate data
        val vars = Seq.tabulate(l)(i => renamedVar(i+1)(getData(i+1)))
        Tuple(vars)
      }
    })
    
    val temp = nit.peek
    Some(Function(temp.domain, temp.range, nit))
  }
  
}

object Pivot extends OperationFactory {
  override def apply(args: Seq[String]): Pivot = new Pivot(args(0), args(1).toInt)
  
  def apply(n: String, i: Int): Pivot = new Pivot(n, i)
}
