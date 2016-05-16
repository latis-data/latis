package latis.ops.agg

import latis.data.value.DoubleValue
import latis.data.value.LongValue
import latis.data.value.StringValue
import latis.dm.Dataset
import latis.dm.Function
import latis.dm.Integer
import latis.dm.Real
import latis.dm.Sample
import latis.dm.Scalar
import latis.dm.Text
import latis.dm.Tuple
import latis.dm.Variable
import latis.ops.OperationFactory
import latis.util.StringUtils
import latis.util.iterator.PeekIterator

/**
 * A Join which includes every Sample from each of the original Datasets. 
 * It does not interpolate or extrapolate, but does use fill values. 
 * If fill values are not defined for any non-domain scalar variables, any sample
 * that needs to use those fill values will be dropped. (eg, if no fills
 * are defined for the right dataset you will get a right join).
 */
class BasicJoin extends Join {
  //TODO: reconcile with FullOuterJoin2
  //  this drops samples instead of filling as advertised
  //NOTE: used only by Resampling for splitting and joining tuple ranges
  
  /**
   * Construct a Variable with values extracted from the "fill_value" metadata.
   * If "fill_value" is not defined for any Variable, the result will be None.
   */
  def getFillVariable(v: Variable): Option[Variable] = v match {
    case s: Scalar => getFillScalar(s)
    case t: Tuple => getFillTuple(t)
    case f: Function => getFillFunction(f)
  }
  def getFillScalar(s: Scalar): Option[Variable] = s.getMetadata("fill_value") match {
    case None => None
    case Some(fv) => Some(s match {
      case i: Integer => i(LongValue(StringUtils.toDouble(fv).toLong))
      case r: Real => r(DoubleValue(StringUtils.toDouble(fv)))
      case t: Text => t(StringValue(fv))
    })
  }
  def getFillTuple(t: Tuple): Option[Variable] = {
    val fills = t.getVariables.map(getFillVariable)
    if(fills.forall(_.nonEmpty)) Some(Tuple(fills.flatten, t.getMetadata))
    else None
  }
  def getFillSample(s: Sample): Option[Sample] = 
    getFillVariable(s.range) match {
    case Some(r) => Some(Sample(s.domain,r))
    case _ => None
  }
  def getFillFunction(f: Function): Option[Variable] = getFillSample(f.getSample) match {
    case Some(s) => Some(Function(Seq(s), f.getMetadata))
    case None => None
  }
  
  def makeSampleIterator(it1: PeekIterator[Sample], it2: PeekIterator[Sample]) = new PeekIterator[Sample] {
    val (fill1, fill2) = (getFillSample(it1.peek), getFillSample(it2.peek))
    
    def getNextOption: Option[Sample] = {
      val comp = (it1.peek, it2.peek) match {
        case (Sample(d1: Scalar, _), Sample(d2: Scalar, _)) => d1.compare(d2)
        case (Sample(d1: Scalar, _), null) => -1
        case (null, Sample(d2: Scalar, _)) => 1
        case (null, null) => Double.NaN
      }
      
      if(comp == 0) {
        joinSamples(it1.next, it2.next)
      }
      else if(comp < 0) fill2 match {
        case None => {it1.next; None} //drop the sample because it can't be filled
        case Some(fill) => {
          val left = it1.next
          joinSamples(left, Sample(left.domain, fill.range))
        }
      } 
      else if(comp > 0) fill1 match {
        case None => {it2.next; None} 
        case Some(fill) => {
          val right = it2.next
          joinSamples(Sample(right.domain, fill.range), right)
        }
      }
      else None
    }
    
    def getNext: Sample = getNextOption match {
      case None => {
        if(it1.hasNext | it2.hasNext) getNext //dropped sample, try again
        else null
      }
      case Some(s) => s
    }
    
  }
  
  def apply(ds1: Dataset, ds2: Dataset): Dataset = {
    if (ds1.isEmpty) ds2
    else if (ds2.isEmpty) ds1
    else (ds1, ds2) match {
      case (Dataset(f1 @ Function(it1)), Dataset(f2 @ Function(it2))) => {
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
        val samples = makeSampleIterator(PeekIterator(it1), PeekIterator(it2))
       
        //TODO: make Function and Dataset metadata
        val pit = samples
        val (domain, range) = pit.peek match {case Sample(d,r) => (d,r)}
        Dataset(Function(domain, range, pit))
      }
    }
  }
  
}

object BasicJoin {
  
  def apply(): BasicJoin = new BasicJoin()
  
  def apply(ds1: Dataset, ds2: Dataset): Dataset = BasicJoin()(ds1, ds2)
}