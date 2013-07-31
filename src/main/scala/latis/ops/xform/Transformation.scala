package latis.ops.xform

import latis.dm._
import latis.config.LatisProperties
import latis.ops.Operation

abstract class Transformation extends Operation {

  def apply(dataset: Dataset): Dataset = Dataset(dataset.variables.map(transform(_)))
  //TODO: provenance metadata...
  
  def transform(variable: Variable): Variable = variable match {
    case s: Scalar => transformScalar(s)
    case t: Tuple => transformTuple(t)
    case f: Function => transformFunction(f)
  }
  
  //type not necessarily preserved
  def transformScalar(scalar: Scalar): Variable = scalar //no-op
  def transformTuple(tuple: Tuple): Variable = Tuple(tuple.variables.map(transform(_)))
  def transformFunction(function: Function): Variable = TransformedFunction(function, this)
    //TODO: or Function(transform(function.domain), transform(function.range)) ?
    
  //typically invoked by TransformedFunction
  def transformSample(sample: Sample): Sample = 
    Sample(transform(sample.domain), transform(sample.range))
  
    /*
     * TODO: if we are operating on function samples is it ever reasonable to NOT return another Sample?
     * domain and range types could change
     * but don't we still need to maintain the same number of samples (unless they are filtered out)?
     * 
     * but may be some operations that can't be applied to each sample
     * boxcar, integration
     * need to use "sliding" on iterator
     * xform must supply that info, when wrapping function
     * 
     * 
     */
    
  /*
   * TODO: 2013-07-01
   * apply operations, as wrapper on orig Variables?
   * wrapper would allow adding ops after construction
   * wrapper might simplify immutability - new Dataset points to orig vars, replacing morphed one with wrapper
   * consider issues of who has the Data
   * +assume we get everything via a Function Iterator, just use MorphedFunctions, morphSample
   * 
   * *general morphing must happen at dataset level, Function may become something completely different
   *  only makes sense to wrap Function for Filters, act on each sample, or group like "sliding"
   *    FilteredFunction (akin to scala collections WithFilter)
   *  could also wrap Tuple with Filter for projected vars
   *    ProjectedTuple?
   *  wrap scalar?
   *  
   * 
   * Like scala, we should support something like 
   *   ds.filter(p: Variable => Boolean) 
   *   ds.map(f: Variable => Variable)
   */
  
    /*
     * relational algebra: projection, selection, rename
     *   projection: list of var names, same as dap2
     *     "project a,b,c"
     *     belongs to Dataset
     *     as opposed to using "include/exclude" in tsml for target var
     *     but "project" is overloaded with "map projections"
     *       or confused with noun "project"
     *   selection: equivalent to true filters, only exclude, no changing of type
     *     always a "filter"?
     *     "filter time > 2" 
     *     or "select ...", too much like SQL, limits thinking? also used for projection in sql
     *   rename: simply give a var a new name, set name in metadata (leave orig id)
     *     "rename foo as bar"?
     * operation: for everything else, typically changes type/metadata
     *   do we need to distinguish between streamable vs operating on the whole thing?
     *   use cases for the latter? 
     *     FFT? appears to be streamable versions
     *   even a single granule is a Stream with one (potentially infinite?) object
     *   streamable doesn't have to mean one sample at a time
     *   e.g. boxcar filter, use scala Seq.grouped, sliding
     * verbs: project, select, rename, operate/transform/morph
     * ?better to use words from theory or practical use?
     *   theory in low level impl, expose common terms in DSL
     *   
     * 
     */
  
}





