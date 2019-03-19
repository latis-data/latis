package latis.ops.filter

import scala.Option.option2Iterable

import latis.dm.Function
import latis.dm.Sample
import latis.dm.Scalar
import latis.dm.Variable
import latis.ops.Operation
import latis.util.DataUtils
import latis.util.iterator.PeekIterator

class NearestNeighborFilter(val domainName: String, val value: String) extends Operation {
  //TODO: any nD domain
  //TODO: if domain type is Integer but value is double
  
  //Note, we do not extend Filter since it drops an outer sample if any innards don't pass
  /*
   * TODO: should this be a Filter?
   * should filter drop entire outer sample?
   * biased by desire to keep same domain set for each nested function (i.e. cartesian)?
   * 
   */
  
  //can't test each sample independently, so operate on Function 
  //see Resample: more complex because it reuses the iterator for multiple samples
  override def applyToFunction(function: Function): Option[Variable] = {
    val domain = function.getDomain  //note, doesn't have the data value we care about
    
    //operate on this function if its domain matches the requested sample
    if (domain.hasName(domainName)) {
      //Iterate until we find the lower bounding sample using peek.
      val fit = PeekIterator(function.iterator)
      fit.find(sample1 => {
        val sample2 = fit.peek //upper bounding sample
        //Check for equality with first sample. If it's the last, sample2 will be null
        val domain1 = sample1.domain.asInstanceOf[Scalar]
        val compare1 = domain1 compare value
        
        /*
         * TODO: fail fast if before 1st sample, 'only' an optimization
         * return true? only way to break out early
         * seems valid, consider case when we want to extrapolate
         * would need special logic after to return None
         * compare1 is out of scope there
         */
        if (compare1 >= 0) true //before or on first sample
        else if (sample2 == null) { false }  //ran out of samples
        else {
          val domain2 = sample2.domain.asInstanceOf[Scalar]
          val compare2 = domain2 compare value
          if (compare1 * compare2 < 0) { true } //diff signs means we found the bounds
          else { false }  //find will try the next pair of samples
        }
      })
      
      //compute the 'distance' from each sample
      //TODO: use scalar math (including max) instead of exposing data?
      //requested domain value, converted from string based on this Function's domain
      val d = DataUtils.parseDoubleValue(domain, value)
      val dd1 = Math.abs(DataUtils.getDoubleValue(fit.current.domain) - d)
      
      //note, if we equal the last sample, peek will be null
      val nearestSample: Sample = if (dd1 == 0.0) { fit.current }
      else if (fit.peek == null) { fit.current } //beyond last sample
      else {
        val dd2 = Math.abs(DataUtils.getDoubleValue(fit.peek.domain) - d)
        //get nearest sample, round 'up'
        if (dd1 < dd2) fit.current else fit.peek
      }
      
      //note, no need to apply operation further since this function was the only thing that matters
      if (nearestSample == null) { None }
      else {
        //set length metadata to 1
        val md = function.getMetadata + ("length" -> "1")
        val f = Function(List(nearestSample), md)
        Some(f)
      }
      
    } else {
      //iterate over samples and apply to each (e.g. nested Functions)
      val newSamples = function.iterator.flatMap(applyToSample(_))
      //TODO: munge metadata
      val f = Function(function, newSamples)
      Some(f)
    }
  }
}

object NearestNeighborFilter {
  
  def apply(vname: String, value: AnyVal): NearestNeighborFilter = NearestNeighborFilter(vname, value.toString)
  
  def apply(vname: String, value: String): NearestNeighborFilter = new NearestNeighborFilter(vname, value)

  def unapply(x: NearestNeighborFilter): Option[(String, String)] =
    Option((x.domainName, x.value))
}
