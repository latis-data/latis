package latis.ops.agg

import latis.ops.BinaryOperation
import latis.dm.Dataset
import latis.ops.resample.NoInterpolation
import latis.dm.Function
import latis.dm.Sample
import latis.dm.Scalar
import scala.collection.mutable.ArrayBuffer
import latis.ops.resample.NoExtrapolation

/**
 * Given two Datasets that each contain a Function with the same domain Variable,
 * create a single Function that contains ALL samples of both Functions using
 * fill values as needed.
 */
class FullOuterJoin extends Join with NoInterpolation with NoExtrapolation {
  //e.g. new FullOuterJoin with LinearInterpolation
  //TODO: diff interpolation for each dataset? from metadata

  private def makeSampleIterator(f1: Function, f2: Function) = new Iterator[Sample]() {
    //define indices of the inner pair of samples in a sliding window
    //used to align each dataset for interpolation
    val upper = interpolationWindowSize / 2
    val lower = upper - 1

    //create the sliding iterators so we can interpolate
    //  within sliding windows
    val it1 = f1.iterator.sliding(interpolationWindowSize).withPartial(false)
    val it2 = f2.iterator.sliding(interpolationWindowSize).withPartial(false)

    //temporary vars for the sliding windows
    var as: Array[Sample] = null //left dataset window
    var bs: Array[Sample] = null //right dataset window

    //hang on to first domain variables so we know when we are done extrapolating
    var firsta1: Scalar = null
    var firstb1: Scalar = null

    //trigger staging the first set of samples on the first pass
    var first = true
    //trigger using extrapolation before interpolation is possible
    var beforeFirstInterp = true

    def hasNext = it1.hasNext || it2.hasNext

    def next: Sample = {
//TODO: premature to test hasNext since we end each iteration with a next
      val oSample: Option[Sample] = if (!it1.hasNext) {
        //no more samples in ds1, extrapolate
        bs = it2.next.toArray
        extrapolate(as, bs(lower).domain.asInstanceOf[Scalar]) match {
          case Some(sample) => joinSamples(sample, bs(lower))
          case None => ??? //fill
        }
      } else if (!it2.hasNext) {
        //no more samples in ds2, extrapolate
        as = it1.next.toArray
        extrapolate(bs, as(lower).domain.asInstanceOf[Scalar]) match {
          case Some(sample) => joinSamples(as(lower), sample)
          case None => ??? 
        }

      } else {
        if (first) {
          //stage the first samples
          as = it1.next.toArray
          bs = it2.next.toArray
          //get 1st values for extrapolation logic
          firsta1 = as(lower).domain.asInstanceOf[Scalar]
          firstb1 = bs(lower).domain.asInstanceOf[Scalar]
          //done with the first pass
          first = false
        }
        //Get the domain variable of the samples to compare
        //Assumes scalar domains, for now
        var a1 = as(lower).domain.asInstanceOf[Scalar]
        var b1 = bs(lower).domain.asInstanceOf[Scalar]
        /*
               * TODO: if beforeFirst then preExtrapolate
               * how to know when to allow interp?
               * sign of a1,b1 compare changes or goes to 0
               * but neither needs to increment
               * a2 > b1
               * a2 = b1 need to inc a
               * use first samples from first block?
               */
        //TODO: Deal with extrapolation until both datasets overlap
        //if (beforeFirstInterp) 
        /*
 * can we increment first then interp
 * allow us to end block with a sample
 * help with "first" logic?
 * OR inc after all cases, should be same logic?
 */
        val joinedSample = if (a1 < b1) interpolate(as, bs(lower).domain.asInstanceOf[Scalar]) match {
          case Some(sample) => joinSamples(sample, bs(lower))
          case None => ??? //fill
        }
        else if (a1 > b1) interpolate(bs, as(lower).domain.asInstanceOf[Scalar]) match {
          case Some(sample) => joinSamples(as(lower), sample)
          case None => ???
        }
        else joinSamples(as(lower), bs(lower))

        //increment iterators as needed
        val a2 = as(upper).domain.asInstanceOf[Scalar]
        val b2 = bs(upper).domain.asInstanceOf[Scalar]
        //Note, a2, b2 are the basis of the next comparison
        if (a2 <= b2 && it1.hasNext) as = it1.next.toArray
        if (a2 >= b2 && it2.hasNext) bs = it2.next.toArray

        joinedSample
      }
      
      oSample match {
        case Some(sample) => sample
        case None => ??? //TODO: next? but may not be a next - orig need for peekIterator
      }
        
    }
  }

  def apply(ds1: Dataset, ds2: Dataset): Dataset = {
    (ds1, ds2) match {
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
        //try without PeekIterator so we don't open data source too soon
 //TODO: can PeekIterator leave data source alone with appropriate use of lazy?
        val samples = makeSampleIterator(f1, f2)

 //TODO: can we get new function type without peeking (i.e. opening data source)?
        
        //TODO: make Function and Dataset metadata
        Dataset(Function(samples.toSeq)) //memoizing samples for now
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
