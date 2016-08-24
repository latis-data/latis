package latis.ops.agg

import latis.ops.BinaryOperation
import latis.dm.Dataset
import latis.ops.resample.NoInterpolation
import latis.dm.Function
import latis.dm.Sample
import latis.dm.Scalar
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.TreeSet
import latis.ops.resample.NoExtrapolation
import latis.util.iterator.PeekIterator
import latis.dm.Number
import latis.dm.Tuple
import latis.dm.Real
import latis.dm.Variable
import latis.data.value.DoubleValue

/**
 * Given two Datasets that each contain a Function with the same domain Variable,
 * create a single Function that contains ALL samples of both Functions using
 * fill values as needed.
 * This implementation assumes that all samples can be held in memory.
 * A common domain Set is created then used to evaluate each Function. 
 * The resulting ranges are joined.
 * Assumes the domain is one dimensional and numeric, for now, and that there
 * are no nested Functions.
 * Missing values will be used for interpolations and extrapolations.
 */
class FullOuterJoin2 extends Join with NoInterpolation with NoExtrapolation {
  
  /*
   * get domain sets, put in same set (no dups) to get final set
   * eval both and join results in new range
   * 
   * assume domain is numeric, use Doubles
   * use TreeMap, sorted
   * assume time: Double -> range (Real or Tuple)
   * join left would be easy - fill just ds2
   *   but we need to go back and fill all
   * 
   * 
   * evaluate = interpolate
   * 
   * 
   * invoke for many datasets (e.g. webtcad download) via POST of URLs to this operation
   * 
   * 
   */
  
  def joinFunctions(f1: Function, f2: Function): Option[Function] = {
    //combine domain values from both into a sorted set
    val domainSet = getDomainSet(f1,f2)
    //define domain metadata, assume both Functions share the same domain metadata
    val origDomain = f1.getDomain
    val domainMetadata = f1.getDomain.getMetadata()
   
    //evaluate both functions for each domain sample
    val samples = domainSet.toSeq.map(d => {
      //construct the domain Variable for this sample
      val domain = origDomain(DoubleValue(d))
      
      //evaluate f1
      val range1 = f1(domain) match {
        case Some(v) => v
        case None => fillMissing(f1.getRange)
      }
      //evaluate f2
      val range2 = f2(domain) match {
        case Some(v) => v
        case None => fillMissing(f2.getRange)
      }
      
      //join the results
      val range = joinVariables(range1, range2) match {
        case Some(v) => v
        case None => throw new Error("Failed to join datasets.")
      }
      
      //construct the resulting sample
      Sample(domain, range)
    })
    
    //TODO: update metadata, get from f1 for now
    val fmd = f1.getMetadata()
    Some(Function(samples, fmd))
  }
  
  private def fillMissing(variable: Variable): Variable = variable match {
    case s: Scalar => Scalar(s.getMetadata, s.getFillValue) //TODO: use copy
    case Tuple(vars) => Tuple(vars.map(fillMissing(_))) //recursive
    case _ => throw new Error("Can't fill missing for Functions.")
  }
  
  private def getDomainSet(f1: Function, f2: Function) = {
    //make a sorted set that contains the domain values of each Function
    val domainSet = TreeSet[Double]()
    //put f1 domain values in sorted set
    f1.iterator.map(_.domain match { case Number(v) => domainSet += v }).toList
    //put f2 domain values in sorted set
    f2.iterator.map(_.domain match { case Number(v) => domainSet += v }).toList
    domainSet
  }
  
  def apply(ds1: Dataset, ds2: Dataset): Dataset = {
    //If one dataset is empty, just return the other
    if (ds1.isEmpty) ds2
    else if (ds2.isEmpty) ds1
    else (ds1, ds2) match {
      case (Dataset(f1: Function), Dataset(f2: Function)) => {
        //make sure the domains are consistent, match on name for now
        if (f1.getDomain.getName != f2.getDomain.getName) {
          val msg = s"Can't join Functions with different domains."
          throw new UnsupportedOperationException(msg)
        }
        //support only Scalar domains for now
        if (!f1.getDomain.isInstanceOf[Scalar] || !f2.getDomain.isInstanceOf[Scalar]) {
          val msg = s"Can't join Functions with non Scalar domains, for now."
          throw new UnsupportedOperationException(msg)
        }

        joinFunctions(f1, f2) match {
          case Some(v) => Dataset(v)
          case None => throw new Error("Failed to join datasets.")
        }
      }
    }
  }
  
//  private def makeSampleIterator(f1: Function, f2: Function) = new PeekIterator[Sample]() {
//    //Define indices of the inner pair of samples in a sliding window.
//    //These are used to align each dataset for interpolation.
//    //TODO: assert even, >=2
//    val upper = interpolationWindowSize / 2
//    val lower = upper - 1
//
//    //Create the sliding iterators so we can interpolate within sliding windows.
//    //Do this lazily so we don't access data until we have to.
//    lazy val pit1 = PeekIterator(f1.iterator.sliding(interpolationWindowSize)) //.withPartial(false))
//    lazy val pit2 = PeekIterator(f2.iterator.sliding(interpolationWindowSize)) //.withPartial(false))
//    
//    //temporary arrays of Samples for the sliding windows
//    var as: Array[Sample] = Array() //left dataset window (a)
//    var bs: Array[Sample] = Array() //right dataset window (b)
//    def firstPass = as.isEmpty  //both should be empty
//    //define extrapolation mode based on 'compare':
//    //  <0 if as are smaller (extrapolating bs)
//    //  >0 if bs are smaller (extrapolating as)
//    var extrapolationMode = 0 
////TODO: can we turn extrapolationMode back on at the end?
//    //diff signs: just use <0 = left = as need extrap
//    //or generalize mode to include interp and done?
//    //use empty as,bs when done?
//    //allow partial window, use length of as,bs?
//    
//    //Populate the next set of windows (as, bs), increment iterators as needed
//    def loadNextWindows = {
//      //get first set of samples if we don't have them yet
//      if (firstPass) {
//        //note, if either is empty, it should have been taken care of by now as an empty dataset
//        as = pit1.next.toArray
//        bs = pit2.next.toArray
//        //if the domain values aren't the same, start in extrapolation mode
//        val a1 = as(lower).domain.asInstanceOf[Scalar]
//        val b1 = bs(lower).domain.asInstanceOf[Scalar]
//        extrapolationMode = b1.compare(a1) // <0 if as need extrap...
//      } else { //not first pass
//        if (extrapolationMode == 0) { //we've been in normal interpolation mode
//          //increment based on upper end of region of interest
//          //a2, b2 are the basis of the next comparison
//     //TODO: can't assume a2 and b2 will be there with partial windows
//          if (as.length < upper + 1) { //partial window (without upper, thus extrapolation needed) near the end of dataset1
//            extrapolationMode = -1
//    //TODO: but still got an inperp to do in B at the last A, do we need to use window size in interp logic below?
//          }
//          val a2 = as(upper).domain.asInstanceOf[Scalar]
//          val b2 = bs(upper).domain.asInstanceOf[Scalar]
////          if (a2 <= b2) {
////            if (pit1.hasNext) as = pit1.next.toArray
////   //TODO: consider partial windows
////            else extrapolationMode = 
////          }
//          
//          if (a2 <= b2 && pit1.hasNext) as = pit1.next.toArray  //TODO: if !hasNext
//          if (a2 >= b2 && pit2.hasNext) bs = pit2.next.toArray  //TODO: if !hasNext
//          //note, both need to advance if the upper values match 
//          //since interp is based on the lower values in the window
//        } else if (extrapolationMode > 0) { //extrap bs
//  //TODO: if we are using etrap mode for end of data, deal with end of both and thus this iterator
//          val a2 = as(upper).domain.asInstanceOf[Scalar]
//          val b1 = bs(lower).domain.asInstanceOf[Scalar]
//          a2.compare(b1) match {
//            case 0 => as = pit1.next.toArray; extrapolationMode = 0  //TODO: if !hasNext, only first and last samples match, need to join them
//            case i: Int if (i < 0) => as = pit1.next.toArray //TODO: if !hasNext, no overlap between datasets, start extrapolating on the end of the other
//            case i: Int if (i > 0) => extrapolationMode = 0
//          }
//        } else if (extrapolationMode < 0) { //extrap as
//          val b2 = bs(upper).domain.asInstanceOf[Scalar]
//          val a1 = as(lower).domain.asInstanceOf[Scalar]
//          b2.compare(a1) match {
//            case 0 => bs = pit2.next.toArray; extrapolationMode = 0 //TODO: if !hasNext, only first and last samples match, need to join them
//            case i: Int if (i < 0) => bs = pit2.next.toArray  //TODO: if !hasNext, no overlap between datasets, start extrapolating on the end of the other
//            case i: Int if (i > 0) => extrapolationMode = 0
//          }
//        }
//      }
//    }
    
    
//    def getNext: Sample = {
//      //Populate the windows of samples (as, bs) for the next joined sample.
//      loadNextWindows
//      
//      //If there are no more samples in either dataset, we are done.
//      //TODO: consider partial windows
///*
// * TODO: haven't used current samples yet so don't use hasNext
// * but need to know if one was already at end and needs exterp
// * add matching null samples at +/- infinity?
// */
//      if (!pit1.hasNext && !pit2.hasNext) null
//      else {
//        //If one dataset has no more samples, extrapolate
//        if (!pit1.hasNext) extrapolate(as, bs(lower).domain.asInstanceOf[Scalar]) match {
//          case Some(sample) => joinSamples(sample, bs(lower))
//          case None => ??? //TODO: fill
//        } else if (!pit2.hasNext) extrapolate(bs, as(lower).domain.asInstanceOf[Scalar]) match {
//          case Some(sample) => joinSamples(as(lower), sample)
//          case None => ??? //TODO: fill
//        }
//        
//        else {
//          //Get the domain variable of the samples to compare
//        //Assumes scalar domains, for now
//        var a1 = as(lower).domain.asInstanceOf[Scalar]
//        var b1 = bs(lower).domain.asInstanceOf[Scalar]
//        }
//      
//        ???
//      }
//    }
//    
//  }
    


//    //hang on to first domain variables so we know when we are done extrapolating
//    var firsta1: Scalar = null
//    var firstb1: Scalar = null
//
//    //trigger staging the first set of samples on the first pass
//    var first = true
//    //trigger using extrapolation before interpolation is possible
//    var beforeFirstInterp = true
//
//    def hasNext = it1.hasNext || it2.hasNext
//
//    def next: Sample = {
////TODO: premature to test hasNext since we end each iteration with a next
//      val oSample: Option[Sample] = if (!it1.hasNext) {
//        //no more samples in ds1, extrapolate
//        bs = it2.next.toArray
//        extrapolate(as, bs(lower).domain.asInstanceOf[Scalar]) match {
//          case Some(sample) => joinSamples(sample, bs(lower))
//          case None => ??? //fill
//        }
//      } else if (!it2.hasNext) {
//        //no more samples in ds2, extrapolate
//        as = it1.next.toArray
//        extrapolate(bs, as(lower).domain.asInstanceOf[Scalar]) match {
//          case Some(sample) => joinSamples(as(lower), sample)
//          case None => ??? 
//        }
//
//      } else {
//        if (first) {
//          //stage the first samples
//          as = it1.next.toArray
//          bs = it2.next.toArray
//          //get 1st values for extrapolation logic
//          firsta1 = as(lower).domain.asInstanceOf[Scalar]
//          firstb1 = bs(lower).domain.asInstanceOf[Scalar]
//          //done with the first pass
//          first = false
//        }
//        //Get the domain variable of the samples to compare
//        //Assumes scalar domains, for now
//        var a1 = as(lower).domain.asInstanceOf[Scalar]
//        var b1 = bs(lower).domain.asInstanceOf[Scalar]
//        /*
//               * TODO: if beforeFirst then preExtrapolate
//               * how to know when to allow interp?
//               * sign of a1,b1 compare changes or goes to 0
//               * but neither needs to increment
//               * a2 > b1
//               * a2 = b1 need to inc a
//               * use first samples from first block?
//               */
//        //TODO: Deal with extrapolation until both datasets overlap
//        //if (beforeFirstInterp) 
//        /*
// * can we increment first then interp
// * allow us to end block with a sample
// * help with "first" logic?
// * OR inc after all cases, should be same logic?
// * but hasNext problem
// * 
// */
//        val joinedSample = if (a1 < b1) interpolate(as, bs(lower).domain.asInstanceOf[Scalar]) match {
//          case Some(sample) => joinSamples(sample, bs(lower))
//          case None => ??? //fill
//        }
//        else if (a1 > b1) interpolate(bs, as(lower).domain.asInstanceOf[Scalar]) match {
//          case Some(sample) => joinSamples(as(lower), sample)
//          case None => ???
//        }
//        else joinSamples(as(lower), bs(lower))
//
//        //increment iterators as needed
//        val a2 = as(upper).domain.asInstanceOf[Scalar]
//        val b2 = bs(upper).domain.asInstanceOf[Scalar]
//        //Note, a2, b2 are the basis of the next comparison
//        if (a2 <= b2 && it1.hasNext) as = it1.next.toArray
//        if (a2 >= b2 && it2.hasNext) bs = it2.next.toArray
//
//        joinedSample
//      }
//      
//      oSample match {
//        case Some(sample) => sample
//        case None => ??? //TODO: next? but may not be a next - orig need for peekIterator
//      }
        
//    }
//  }



  /*
   * TODO: how could we do this in the context of resampling
   * Discrete telemetry should be filled with previous value: FloorResampling
   *   hard to implement as a fill value
   * Other data would generally use NoResampling and use a static fill value.
   * 
   * iterator of samples from each ds
   * peek at each
   * take min and resample other to that point
   * could add "same" logic but resample would just work
   * can we maintain previous value?
   *   not with a PeekIterator
   * what about resampling that uses more samples
   * seems like we need to support a running window for even 2 points
   * 
   * consider visad's approach: Set.indexOf(value)
   * but we are moving away from separate domain and range sets
   * 
   * SampledFunction has a Resampling strategy (akin to Ordering?)
   * Resampling trait/mixin
   *   iterate over samples with "sliding"
   *   
   * Instead of using peek, iterate over samples of 2 functions (a,b)
   * with sliding = 2 (1,2)
   * assume no extrapolation
   * use Interpolation and Extrapolation traits
   * 
   * a1 * * b1
   * a2 * 
   *      * b2
   * 
   * cases: (comparing domain values of samples)
   * a1 = b1 => join
   *      a2 = b2 => inc a,b (join in next iteration)
   *      a2 < b2 => inc a
   *      a2 > b2 => inc b
   * a1 < b1 => interp a at b1
   *      a2 = b2 => inc a,b
   *      a2 < b2 => inc a
   *      a2 > b2 => inc b
   * a1 > b1 => interp b at a1
   *      a2 = b2 => inc a,b
   *      a2 < b2 => inc a
   *      a2 > b2 => inc b
   * every step will result in a sample
   * extrap logic only at start and end
   * assume none for now?
   * 
   * what about using interp with larger window?
   *   n=4: a1, a2, a3, a4
   *   use a2 (lower) for interp test, a3 (upper) for inc test
   *   a{n/2}, a{n/2+1}
   *   a(n/2-1), a(n/2)
   *   assume even?
   * will sign just work
   */
}

object FullOuterJoin2 {
  def apply() = new FullOuterJoin2()
}
