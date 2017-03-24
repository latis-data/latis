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
 * interpolated or extrapolated values as needed.
 * The default Interpolation and Extrapolation strategies will result in fill values.
 */
class FullOuterJoin extends Join with NoInterpolation with NoExtrapolation {
  //e.g. new FullOuterJoin with LinearInterpolation
  //TODO: diff interpolation for each dataset? from metadata

  /**
   * Construct a PeekIterator of Samples that result from joining Samples
   * from the given Functions. Interpolation or Extrapolation with be used
   * to fill data in the "left" and "right" Datasets.
   */
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
    //and set extrapolationMode and/or noMoreData as needed.
    def loadNextWindows = {
      //get first set of samples if we don't have them yet, and establish the extrapolation mode
      if (firstPass) {
        //note, if either is empty, it should have been taken care of by now as an empty dataset
        as = pit1.next.toArray
        bs = pit2.next.toArray
        //if the domain values aren't the same, start in extrapolation mode
        val a1 = as(lower).domain
        val b1 = bs(lower).domain
        extrapolationMode = a1.compare(b1) match { // <0 if Bs need extrap...
          case 0 => {
            //TODO: need to test with windows > 2
            if (as.length <= upper && bs.length <= upper) noMoreData = true
            "none"
          }
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
            if (as.length == upper) {
              //out of As, increment Bs
              extrapolationMode = "postA"
              if (pit2.hasNext) bs = pit2.next.toArray
              else {
                bs = bs.tail
                noMoreData = true
              }
            } else if (bs.length == upper) {
              //out of Bs, increment As
              extrapolationMode = "postB"
              if (pit1.hasNext) as = pit1.next.toArray
              else {
                as = as.tail
                noMoreData = true
              }
            } else {
  
              //The domain values of a2, b2 are the basis of the next comparison.
              val a2 = as(upper).domain
              val b2 = bs(upper).domain

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
                    case (true, false) =>
                      as = pit1.next.toArray; bs = bs.tail //; extrapolationMode = "postB"
                    case (false, true) => bs = pit2.next.toArray; as = as.tail //; extrapolationMode = "postA"
                    //note: because a2 = b2, the "next" for A will align a1 with the last B so we are not quite ready to start extrapolating yet
                  }
                }

                case i: Int if (i < 0) => { //a2 < b2
                  if (pit1.hasNext) as = pit1.next.toArray
                  else as = as.tail
                }

                case i: Int if (i > 0) => { //a2 > b2
                  if (pit2.hasNext) bs = pit2.next.toArray
                  else if (bs.length == upper) extrapolationMode = "postB"
                  //already shortened, at the end

                  else bs = bs.tail
                }
              }
            }
          }

          //--- PreA extrapolation mode ---//
          case "preA" => {
            if (bs.length == upper) {
              //out of Bs
              extrapolationMode = "postB"
              if (as.length == upper) noMoreData = true //also out of As
            } else {
              val a1 = as(lower).domain
              val b2 = bs(upper).domain
              a1.compare(b2) match {
                case i: Int if (i > 0) => {
                  if (pit2.hasNext) bs = pit2.next.toArray //stay in preA mode
                  else bs = bs.tail
                }
                case i: Int if (i < 0) => extrapolationMode = "none" //ready to interpolate Bs to get a value for A
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
          }

          //--- PreB extrapolation mode ---//
          case "preB" => {
            if (as.length == upper) {
              //out of As
              extrapolationMode = "postA"
              if (bs.length == upper) noMoreData = true //also out of Bs
            } else {
              val a2 = as(upper).domain
              val b1 = bs(lower).domain
              a2.compare(b1) match {
                case i: Int if (i < 0) => {
                  if (pit1.hasNext) as = pit1.next.toArray //stay in preB mode
                  else as = as.tail
                }
                case i: Int if (i > 0) => extrapolationMode = "none" //ready to interpolate As to get a value for B
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
    
    //Get the next sample for this PeekIterator.
    def getNext: Sample = {
      //TODO: consider interpolationWindowSize > 2
      if (noMoreData) null
      else {
        //Populate the windows of samples (as, bs) for the next joined sample.
        loadNextWindows

        //Create the next sample by interpolation or extrapolation
        //based on the As and Bs prepared by "loadNextWindow"
        //and the extrapolationMode.
        val joinedSample = {
          //Get the domain variable of the samples to compare
          //Assumes scalar domains, for now
          var a1 = as(lower).domain.asInstanceOf[Scalar]
          var b1 = bs(lower).domain.asInstanceOf[Scalar]
          
          //Extrapolate a value for A
          if (extrapolationMode.endsWith("A")) extrapolate(as, b1) match {
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
          else if (a1 > b1) interpolate(bs, a1) match {
          //else interpolate(bs, a1) match {
            case Some(sample) => joinSamples(as(lower), sample)
            case None => ???
          }
          
          //Domain values match, simply join the ranges
          else joinSamples(as(lower), bs(lower))
        }
        
        //The PeekIterator getNext expects the next element of null.
        //TODO: use Option?
        joinedSample match {
          case Some(sample) => sample
          case None => null
        }
      }
    }
    
  }

  /*
   * TODO: inherit bulk of this from Join
   * TODO: look for matching Functions not just at the top
   * 
   */
  def apply(ds1: Dataset, ds2: Dataset): Dataset = {
    //If one dataset is empty, just return the other
    if (ds1.isEmpty) ds2 //only if no variable
    else if (ds2.isEmpty) ds1
    //TODO: both empty
    else (ds1, ds2) match {
      case (Dataset(f1: Function), Dataset(f2: Function)) => {
        (f1.isEmpty, f2.isEmpty) match {
        //(f1.iterator.hasNext, f2.iterator.hasNext) match { //TODO: can't call twice
          case (true, true) => Dataset.empty
  //        case (true, false) => ds2 //TODO: the result of a join come up "empty" here: three_datasets
          case (false, true) => ds1

          case _ => {
            //make sure the domains are consistent, match on name for now
            if (f1.getDomain.getName != f2.getDomain.getName) {
              //TODO: consider aliases
              //TODO: check units, same or convertable
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
            //TODO: pass Iterator[Sample]? but remain lazy

            //TODO: make Function and Dataset metadata
            //TODO: add this peek trick to the Function constructor for iterator?
            val pit = PeekIterator(samples)
            val (domain, range) = pit.peek match { case Sample(d, r) => (d, r) }
            Dataset(Function(domain, range, pit))
          }
        }
      }

      case _ => {
        val msg = "Datasets must contain only Functions, for now."
        throw new UnsupportedOperationException(msg)
      }
    }
  }

  /*
   * 
   *   
   * Iterate over samples of 2 functions (a,b)
   * with sliding = 2 (1,2)
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
   * 
   * using interp with larger window
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