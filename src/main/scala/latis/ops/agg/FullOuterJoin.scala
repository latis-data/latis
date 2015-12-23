package latis.ops.agg

import latis.ops.BinaryOperation
import latis.dm.Dataset
import latis.ops.resample.NoInterpolation
import latis.dm.Function
import latis.dm.Sample
import latis.dm.Scalar

/**
 * Given two Datasets that each contain a Function with the same domain Variable,
 * create a single Function that contains ALL samples of both Functions using 
 * fill values as needed.
 */
class FullOuterJoin extends BinaryOperation with NoInterpolation { //TODO: with NoExtrapolation {
  //e.g. new FullOuterJoin with LinearInterpolation
  //TODO: diff interpolation for each dataset? from metadata
  
  def apply(ds1: Dataset, ds2: Dataset): Dataset = {
    (ds1, ds2) match {
      case(Dataset(f1: Function), Dataset(f2: Function)) => {
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
        
        //TODO: factor up common join stuff
        
        //define indices of the inner pair of samples in a sliding window
        //used to align each dataset for interpolation
        val upper = interpolationWindowSize / 2
        val lower = upper - 1
        
        //create the sliding iterators so we can interpolate
        //  within sliding windows
        val it1 = f1.iterator.sliding(interpolationWindowSize)
        val it2 = f2.iterator.sliding(interpolationWindowSize)
        
        //loop for each sample we need to produce
        var notDone = it1.hasNext && it2.hasNext
        while (notDone) {
          var as: List[Sample] = it1.next
          var bs: List[Sample] = it2.next
          //if starting with a1 < b1 extrap b and inc a until a1 >= b1
          //assuming Scala domain for now //TODO: need to compare ordering of domain Tuples
          var a1 = as(lower).domain.asInstanceOf[Scalar]
          var b1 = bs(lower).domain.asInstanceOf[Scalar]
          while (a1 < b1) {
            //TODO: extrapolate f2
            if (it1.hasNext) {
              as = it1.next
              a1 = as(lower).domain.asInstanceOf[Scalar]
            } 
            else notDone = false //hit end of one before it ever got caught up  //TODO: need to skip some stuff
          }
          //TODO: b1 < a1
          
          //we should now have a set of as and bs to do interp
          //strip down to doubles
          //interp for each parameter in range
         // val interp = interpolator()
          
        }
        
        ???
      }
      case _ => throw new UnsupportedOperationException("FullOuterJoin expects a Function in each of the Datasets it joins.")
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
   * a1 > b1 => interp a at b1
   *      a2 = b2 => inc a,b
   *      a2 < b2 => inc a
   *      a2 > b2 => inc b
   * a1 < b1 => interp b at a1
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