package latis.ops.agg

import latis.ops.BinaryOperation
import latis.dm.Dataset
import latis.ops.resample.NoInterpolation
import latis.dm.Function
import latis.dm.Sample
import latis.dm.Scalar
import scala.collection.mutable.ArrayBuffer
import latis.ops.resample.NoExtrapolation
import latis.util.iterator.PeekIterator
import latis.dm.Number
import latis.dm.TupleMatch

/**
 * Given two Datasets that each contain a Function with the same domain Variable,
 * create a single Function that contains ALL samples of both Functions using
 * fill values as needed.
 */
class FullOuterJoin extends Join with NoInterpolation with NoExtrapolation {
  //e.g. new FullOuterJoin with LinearInterpolation
  //TODO: diff interpolation for each dataset? from metadata
  //TODO: would it be easier to have a single "interp" that does both interp and extrap? avoid extrapolationMode?

  private def makeSampleIterator(f1: Function, f2: Function) = new PeekIterator[Sample]() {
    //Define indices of the inner pair of samples in a sliding window.
    //These are used to align each dataset for interpolation.
    //TODO: assert even, >=2
    val upper = interpolationWindowSize / 2
    val lower = upper - 1

    var noMoreData = false //interpolationWindowSize / 2 samples left, assume 1 for now
    
    //Create the sliding iterators so we can interpolate within sliding windows.
    //Do this lazily so we don't access data until we have to.
    lazy val pit1 = PeekIterator(f1.iterator.sliding(interpolationWindowSize))
    lazy val pit2 = PeekIterator(f2.iterator.sliding(interpolationWindowSize))
    
    //temporary arrays of Samples for the sliding windows
    var as: Array[Sample] = Array() //left dataset window (a)
    var bs: Array[Sample] = Array() //right dataset window (b)
    def firstPass = as.isEmpty  //both should be empty
    
    /**
     * Extrapolation mode:
     * none
     * preA
     * preB
     * postA
     * postB
     */
    var extrapolationMode = "none"
    
    //Populate the next set of windows (as, bs), increment iterators as needed
    def loadNextWindows = {
      //get first set of samples if we don't have them yet, and establish the extrapolation mode
      if (firstPass) {
        //note, if either is empty, it should have been taken care of by now as an empty dataset
 //***TODO: empty Function not seen as empty dataset
   //TODO: use noA and noB for mode to skip join?
        //TODO: can we do the first pass above to avoid this firstPass logic yet still be lazy?
        //TODO: can we use lengths here to determine post mode? a1=b1 will ignore mode
        //  but single sample problem: can't be pre and post at same time
        //  do we need special case for dataset with one sample? would like to think not
        //  but partial window is problematic
        as = pit1.next.toArray
        bs = pit2.next.toArray
        //if the domain values aren't the same, start in extrapolation mode
        val a1 = as(lower).domain
        val b1 = bs(lower).domain
        extrapolationMode = a1.compare(b1) match { // <0 if Bs need extrap...
          case 0 => "none"
          case i: Int if (i < 0) => "preB"
          case i: Int if (i > 0) => "preA"
        }
        
      } else { 
        //not first pass, prepare samples based on extrapolation mode
        extrapolationMode match {
          
          //--- Not in extrapolation mode ---//
          case "none" => {
            //we've been in normal interpolation mode
            //increment based on upper end of region of interest
            //The domain values of a2, b2 are the basis of the next comparison
            val a2 = as(upper).domain
            val b2 = bs(upper).domain
  //TODO: should we deal with near the end here?
            
            a2.compare(b2) match {
              case 0 => {
                //need to advance both, deal with potential end of samples
                (pit1.hasNext, pit2.hasNext) match {
                  case (true, true) => {
                    as = pit1.next.toArray
                    bs = pit2.next.toArray
                  }
                  case (false, false) => {
                    //both datasets end on the same sample
                    //since there is no "next" get the same effect by dropping the first sample (while leaving a partial window)
                    as = as.tail
                    bs = bs.tail
                    noMoreData = true //let getNext know that we are on the last sample
                  }
                  case (true, false) => as = pit1.next.toArray; bs = bs.tail; extrapolationMode = "postB"
                  case (false, true) => bs = pit2.next.toArray; as = as.tail; extrapolationMode = "postA"
                  //note: because a2 = b2, the "next" for A will align a1 with the last B so we are not quite ready to start extrapolating yet
                }
              }
              
              case i: Int if (i < 0) => { //a2 < b2
                if (pit1.hasNext) as = pit1.next.toArray
                else as = as.tail
              }
              
              case i: Int if (i > 0) => { //a2 > b2
                if (pit2.hasNext) bs = pit2.next.toArray
                else bs = bs.tail
              }
            }
          }
          
          //--- PreA extrapolation mode ---//
          case "preA" => {
            val a1 = as(lower).domain
            val b2 = bs(upper).domain
            a1.compare(b2) match {
              case i: Int if (i > 0) => bs = pit2.next.toArray  //stay in preA mode
              case i: Int if (i < 0) => extrapolationMode = "none"  //ready to interpolate Bs to get a value for A
              case 0 => {
                extrapolationMode = "none" //advance b1 to b2; a1=b1 so join regardless of extrap mode
                if (pit2.hasNext) bs = pit2.next.toArray
                else {
                  bs = bs.tail
                  if (as.length < interpolationWindowSize) noMoreData = true //last of the As and Bs
                }
              }
            }
          }
          
          //--- PreB extrapolation mode ---//
          case "preB" => {
            val a2 = as(upper).domain
            val b1 = bs(lower).domain
            val z = a2.compare(b1) 
            z match {
              case i: Int if (i < 0) => as = pit1.next.toArray  //stay in preB mode
              case i: Int if (i > 0) => extrapolationMode = "none"  //ready to interpolate As to get a value for B
              case 0 => {
                extrapolationMode = "none" //advance a1 to a2; a1=b1 so join regardless of extrap mode
                if (pit1.hasNext) as = pit1.next.toArray
                else {
                  as = as.tail
                  if (bs.length < interpolationWindowSize) noMoreData = true //last of the As and Bs
                }
              }
            }
          }
          
          //--- PostA extrapolation mode ---//
          case "postA" => {
            if (pit2.hasNext) bs = pit2.next.toArray
            else {
              bs = bs.tail
              noMoreData = true
            }
          }
          
          //--- PostB extrapolation mode ---//
          case "postB" => {
            if (pit1.hasNext) as = pit1.next.toArray
            else {
              as = as.tail
              noMoreData = true
            }
          }
        }
        
      }
    }
    
    
    def getNext: Sample = {
      //TODO: if noMoreData, we need to do interpolationWindowSize / 2 more samples
      //let's just assume a window of 2 for now
      if (noMoreData) null
      else {
        //If we ran out of samples last time, set the etrapolation mode for this sample
        //TODO: only tested with window size of 2
 //       if (as.length < interpolationWindowSize) extrapolationMode = "postA"
 //       if (bs.length < interpolationWindowSize) extrapolationMode = "postB"

        //Populate the windows of samples (as, bs) for the next joined sample.
        loadNextWindows
        
println("getNext")
println("as: " + as.map(_.domain.getNumberData.doubleValue).mkString(" "))
println("bs: " + bs.map(_.domain.getNumberData.doubleValue).mkString(" "))
println("extrap: " + extrapolationMode)  
println("more data: " + !noMoreData)  

        val joinedSample = {
          //Get the domain variable of the samples to compare
          //Assumes scalar domains, for now
          var a1 = as(lower).domain.asInstanceOf[Scalar]
          var b1 = bs(lower).domain.asInstanceOf[Scalar]
          
          
          
          
          //Domain values match
          //Do this first so we aren't impacted by extrapolation mode.
          if (a1 == b1) joinSamples(as(lower), bs(lower))
  /*
   * TODO: similar problem for one_sample_inside_first  
   * a2     length test puts us in postB mode even though we can do an interp
   *    b1  can't simply do after the interp because it can still get set in an earlier preB pass
   * a1
   * 
   * can we determine extrap mode here based on As and Bs instead of setting it elsewhere?
   * loadNextWindows prepares A, B windows based on mode
   * this interp/extrap based on As, Bs; best place to define extrap mode
   * but we already left the old position behind
   *   we can't know if a1<b1 here needs extrap because we don't know if there was anything before a1
   *   the "none" mode assumes no need to worry about lower values
   */
          //Extrapolate a value for A
          else if (extrapolationMode.endsWith("A")) extrapolate(as, b1) match {
            case Some(sample) => joinSamples(sample, bs(lower))
            case None => ??? //error?
          }
          //Extrapolate a value for B
          else if (extrapolationMode.endsWith("B")) extrapolate(bs, a1) match {
            case Some(sample) => joinSamples(as(lower), sample)
            case None => ??? //error?
          }
            
          //Need to generate an "a" sample at the value of b1
          else if (a1 < b1) interpolate(as, b1) match {
            case Some(sample) => joinSamples(sample, bs(lower))
            case None => ??? //error? interpolation (even fill) is not supported
          }
          
          //Need to generate a "b" sample at the value of a1
          //else if (a1 > b1) interpolate(bs, a1) match {
          else interpolate(bs, a1) match {
            case Some(sample) => joinSamples(as(lower), sample)
            case None => ???
          }
        }
        
        joinedSample match {
          case Some(sample) => sample
          case None => null
        }
      }
    }
    
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
        //make Iterator of new Samples
        val samples = makeSampleIterator(f1, f2)
       
        //TODO: make Function and Dataset metadata
        //TODO: add this peek trick to the Function constructor for iterator?
        val pit = PeekIterator(samples)
        val (domain, range) = pit.peek match {case Sample(d,r) => (d,r)}
        Dataset(Function(domain, range, pit))
      }
    }
  }

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

object FullOuterJoin {
  def apply() = new FullOuterJoin()
}