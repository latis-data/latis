package latis.dm

/**
 * This Variable represents a single sample of a Function of the same type.
 * As a pair of domain and range "values", it is a special kind of Tuple.
 */
class Sample(val domain: Variable, val range: Variable) extends AbstractTuple(List(domain,range)) 
//TODO: add metadata, data?
//TODO: require getDomain, getRange?

object Sample {
  def apply(domain: Variable, range: Variable) = new Sample(domain, range)
  
  def unapply(sample: Sample): Option[(Variable, Variable)] = Some((sample.domain, sample.range))
}