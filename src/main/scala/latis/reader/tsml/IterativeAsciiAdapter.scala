package latis.reader.tsml

import latis.dm._
import scala.io.Source
import scala.collection._
import latis.data.Data
import latis.util.Util
import java.nio.ByteBuffer
import latis.data.IterableData
import latis.time.Time
import latis.reader.tsml.ml.Tsml
import latis.util.PeekIterator
import latis.util.StringUtils

class IterativeAsciiAdapter(tsml: Tsml) extends IterativeAdapter(tsml) with AsciiAdapterHelper {
  
  /*
   * TODO: support nested Function
   * should a record have all lines for an outer sample? probably yes
   * consider db result set style (below) vs flattened to one row
   * would need to get inner function length
   * or should adapter just read that many lines when it gets to it?
   * function length should account for length of nested function
   * (x,y)->a equivalent to x->y->a so number of records should be number of 'a' values?
   *   or at least 'length' should be
   * 
   * could we use algebra?
   *   read n (3) records
   *   make sure t is same for all
   *  takeWhile range.head has same value
   *   i -> (t,w,a,b)
   *  groupBy(w)
   *   w -> (a,b)
   * something like groupBy that removes var from range
   *   extract? factorOut?
   *   or just make that the behavior of groupBy
   *   scala groupBy takes function, this one takes var name
   *   we could offer one like scala that would keep same range since domain could be new var
   *   +use this to make time domain from year,mon,day columns?
   *     i -> (y,m,d,a) groupBy (y,m,d)=>t  =>  t -> (y,m,d,a), still would like to drop y,m,d from range?
   *   if no nested function and var is a suitable domain var, groupBy should have one value for each domain sample
   *   with nested function, groupBy should give seq, like typical scala groupBy
   *   +in this case, groupBy semantics don't make sense
   *   *factorOutDomain?
   *   semantics of removing from range to domain
   *   only allow for IndexFunctions?
   *   more than one arg means multi-dim
   *   
   *   i -> (t,w,a,b) factorOutDomain("t") => t -> (w,a,b)
   *   but not just one sample for each time: t -> (i -> (w.a.b)), more consistent with groupBy
   *   now the inner function is an index function
   *   for each time sample call fOD("w") on range: t -> (w -> (a.b))
   *   +should fOD(x) always have IndexFunction as range even if only one sample?
   *   'reduce' IndexFunction of length one?
   *     but only if for all time samples ?
   *     could we have t -> (a | i -> a) ?
   *     for now at least, lets say reduce can reduce range only if all samples can be reduced
   *   or should fOD only work when it is a valid domain, have to use both vars
   *   fOD(t,w), but get 2D domain instead of nested
   *   groupByAndFactorOut?
   *     but presumably factoring will do that since you can't have duplicate domain values
   *   
   * groupBy with Iterative adapters
   *   must read all, in generic case
   *   if it's a domain var, we could takeWhile val is the same
   *   use factorOutDomain for that special case?
   *   
   * +groupByName vs value, current groupBy only uses name
   *   
   * +factor implies multiplication
   *   is some abstract type of multiplication involved here?
   *   t: 1,2,3  a: A,B,C
   *   kind of like dot product
   *   
   *   
   * *keep in mind Data issues: column vs row oriented
   * 
   * *keep in mind principle of modeling the *source* dataset
   *   better to use ops (algebra) to manipulate it than to try to get clever with the adapter
   */
//1970/01/01  1 1.1 A
//1970/01/01  2 2.2 B
//1970/01/01  3 3.3 C
//1970/01/02  1 11.1 A
//1970/01/02  2 12.2 B
//1970/01/02  3 13.3 C
//1970/01/03  1 21.1 A
//1970/01/03  2 22.2 B
//1970/01/03  3 23.3 C
  
  def makeIterableData(sampleTemplate: Sample): Data = new IterableData {
    def recordSize = sampleTemplate.getSize
    
    def iterator = new PeekIterator[Data] {
      val it = getRecordIterator
      
      //keep going till we find a valid record or run out
      def getNext: Data = {
        if (it.hasNext) {
          val record = it.next
          val svals = parseRecord(record)
          if (svals.isEmpty) getNext  //TODO: log warning? or in parseRecord
          else makeDataFromRecord(sampleTemplate, svals)
        } else null
      }
    }
  }
  
  /**
   * Counter in case we have an Index domain.
   */
  private var index = -1
  
  //TODO: compare to Util dataToVariable... move this there?
  def makeDataFromRecord(sampleTemplate: Sample, svals: Map[String, String]): Data = {
    //build a ByteBuffer
    val size = sampleTemplate.getSize
    val bb = ByteBuffer.allocate(size)
    
    //assume every Scalar in the template has a value in the Map (except index), e.g. not stored by Tuple name
    //get Seq of Scalars from template
    val vars = sampleTemplate.toSeq 
    
    for (v <- vars) {
      v match {
        case _: Index   => index += 1; bb.putInt(index) //deal with index domain (defined in tsml)
        case t: Text    => {
          val s = StringUtils.padOrTruncate(svals(v.getName), t.length)
          s.foldLeft(bb)(_.putChar(_)) //fold each character into buffer
        }
        //Note, the numerical data will attempt to use a fill or missing value if it fails to parse the string
        case _: Real    => bb.putDouble(v.stringToValue(svals(v.getName)).asInstanceOf[Double])
        case _: Integer => bb.putLong(v.stringToValue(svals(v.getName)).asInstanceOf[Long])
      }
    }
    
    //rewind for use
    Data(bb.flip.asInstanceOf[ByteBuffer])
  }

}