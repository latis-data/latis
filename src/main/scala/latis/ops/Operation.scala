package latis.ops

import latis.dm._
import latis.util.LatisProperties

trait Operation {

  def apply(dataset: Dataset): Dataset

  //  protected def applyToVariable(variable: Variable): Variable = variable match {
  //    case s: Scalar => applyToScalar(s)
  //    case t: Tuple => applyToTuple(t)
  //    case f: Function => applyToFunction(f)
  //  }
  //  
  //  //type not necessarily preserved
  //  protected def applyToScalar(scalar: Scalar): Variable = scalar //no-op
  //  protected def applyToTuple(tuple: Tuple): Variable = Tuple(tuple.variables.map(applyToVariable(_)))
  //  protected def applyToFunction(function: Function): Variable = WrappedFunction(function, this)
  //    //TODO: or Function(applyToVariable(function.domain), applyToVariable(function.range)) ?
  //    
  //  //typically invoked by WrappedFunction
  //  protected def applyToSample(sample: Sample): Variable = 
  //    Sample(applyToVariable(sample.domain), applyToVariable(sample.range))

  /*
     * TODO: if we are operating on function samples is it ever reasonable to NOT return another Sample?
     * domain and range types could change
     * but don't we still need to maintain the same number of samples (unless they are filtered out)?
     * 
     * but may be some operations that can't be applied to each sample
     * ++need to keep Filter and Operation unique?
     *   but still have common superclass - Trait
     *   apply(ds):ds
     * Transformation?
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

    /*
     * Operations: subclass of Operation for each? easier to reason about
     *   Selection: exclude Function samples, affect only provenance metadata
     *     needs to be given a predicate: Sample => Boolean
     *     can we use: filterSample(samp) => Option ?
     *     or is there a more idiomatic way?
     *     Function as Seq[Sample], iterator is Iterator[Sample]
     *     selection as function instead of impld in "filter" method
     *     f.iterator.filter(selection)
     *     but we are implementing the iterator using getNext, need to apply filter then?
     *       want to know if there are more samples, so we need NextIterator to make sure there are more valid samples
     *       other ops don't need to worry about this since they aren't filtering out samples
     *       filter(v) => Option seems fine
     *     Selection extend Function1, Sample => Boolean?
     *   Projection: exclude Variables from the model, affect only provenance metadata
     *     do before reading data
     *     may need to be clever if data not column oriented, exclude interleaved data values
     *     predicate: Variable => Boolean
     *     based entirely on Variable name
     *     iterating over model components, not data samples
     *     no need to provide Function wrapper?
     *       what about extracting data from the orig sample
     *       might work for col-oriented (only scalars have data) 
     *       but record-oriented might need to apply to each sample
     *       redundant but easier for interleaved data?
     *       maybe wrap data? or just use name => index
     *       would like to avoid parsing data we just throw away
     *       adapter responsibility?
     *       since sample is just a Tuple, the filtering should just work
     *     filterFunction:
     *       if f has data, use Wrapped Function
     *         filter will be applied to each sample as read
     *         still need to apply to model (domain, range)
     *         can we do during construction so we can pass these to super constructor?
     *         ugly, deal with Option...
     *         replace all constructor args with defs for Function...?
     *         ++seems like the only way if we want Wrappedfunction to BE-A Function
     *           especially given potential complications of general Transromation
     *       else, iterator will get data from kids (column-oriented)
     *         could still just filter samples, but wasting effort parsing all data
     *     ++is an empty Tuple allowed?
     *   Transformation
     *     ++need a type of op that doesn't change type/model?
     *       e.g. unit conversion, basic math
     *       change values (and metadata)
     *       do we need a subclass? easier to reason about?
     *     traits for ModelChanging, DataChanging, ...
     *     
     *   ("Rename" is also part of relational algebra)
     * 
     * Keep in mind diffs between applying ops to existing dataset vs applied by adapter during construction
     * consider function composition, pipe/decorate
     * 
     * Things that can change in a Dataset operation
     * - model
     * - number of samples
     * - data values
     * - metadata
     * sample ops
     * - exclude Variable(s) (reduce model, domain Vars replaced by Index)
     * - select/filter (exclude samples, same type)
     * - subset/resample (diff set of samples, same type)
     * - reduce dimension lengths (stride, bin, ...)
     * - reduce dimension to length to 1, eliminate? (slice, first, last, min, max, integrate)
     * - convert units (change values, change metadata, same model/type)
     * - replace values (change values, e.g. missing value, may change metadata, same model)
     * - compute new variable (e.g. B mag, same domain, add to model)
     * - new variable with new domain type (e.g. FFT, completely new dataset, incompatible domains)
     * - rename (metadata only)
     * - enhance/repair metadata
     * 
     * can we always change model or metadata without having to read data?
     *   "length" is often handy, but ok if that requires processing the data
     * 
     * Do we ever need to extend Selection or Projection?
     * probably not
     */

}

object Operation {
  
  def apply(name: String): Operation = Operation(name, List(""))

  def apply(name: String, args: Seq[String]): Operation = {
    LatisProperties.get("operation." + name + ".class") match {
      case Some(cname) => {
       try {
        val cls = Class.forName(cname)
        if (args.head.length == 0) {  //no args
          val ctor = cls.getConstructor()
          ctor.newInstance().asInstanceOf[Operation]
        } else {
          val ctor = cls.getConstructor(classOf[Seq[String]])
          ctor.newInstance(args).asInstanceOf[Operation]
        }
       } catch {
         case e: Exception => throw new RuntimeException("Unsupported Operation: " + name, e)
       }
      }
      case None => throw new RuntimeException("Unsupported Operation: " + name)
    }
  }

}






