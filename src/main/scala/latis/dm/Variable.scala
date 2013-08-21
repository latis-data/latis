package latis.dm

import latis.data._
import latis.metadata._
import latis.ops.math.BasicMath

//abstract class Variable(val metadata: Metadata, val data: Data) {
abstract class Variable {
  //TODO: with Subsetting,... so we don't end up with so much code here

  protected var _metadata: Metadata = EmptyMetadata
  protected var _data: Data = EmptyData
  
  //allow subclasses to override, e.g. ProjectedFunction
  def metadata: Metadata = _metadata
  def data: Data = _data
  
  lazy val name = metadata.name
  
  /**
   * Size of this Variable in bytes.
   */
  lazy val size: Int = getSize
  protected def getSize = this match {
    case r: Real => 8 //TODO: override for Scalars to support custom?
    //TODO: other types
    case Tuple(vars) => vars.foldLeft(0)(_ + _.size)
    case f @ Function(d,r) => f.length * (d.size + r.size)
  }
  
  /**
   * Get the size (in bytes) of one sample of this Variable.
   * Nested Functions will be counted in their entirety.
   * This will count siblings within a Tuple (e.g. Dataset)
   * so it does not assume a single top level Function only.
   * If there are multiple Functions within a Tuple (allowed?),
   * this will count the sample size for each. 
   * This is problematic if they have different lengths 
   * and/or incompatible domain sets.
   */
  lazy val sampleSize: Int = getSampleSize
  protected def getSampleSize: Int = this match {
    case Function(d,r) => d.size + r.size 
    case _ => size //default to size for Scalars and Tuples
  }
  
  /**
   * Return this Variable as a "flattened" Seq of Scalars.
   * In other words, considering the Variable as a tree,
   * return all the leaves in depth first order.
   */
  def toSeq: Seq[Scalar] = this match {
    case s: Scalar => Seq(s)
    case Tuple(vars) => vars.foldLeft(Seq[Scalar]())(_ ++ _.toSeq)
    case Function(d,r) => d.toSeq ++ r.toSeq
  }
  
  
//  def getVariableByIndex(index: Int): Variable = this match {
//    case s: Scalar => {
//      if (index == 0) s
//      else throw new IndexOutOfBoundsException()
//    }
//    
//    case Tuple(vars) => vars(index)
//    //TODO: if Tuple had data, need to split it up? or expose slice via WrappedData
//    
//    case Function(domain, range) => Function(domain, range.getVariableByIndex(index))
//    //TODO: deal with Data if higher level structures have data, e.g. FilteredFunction?
//  }
  
  
//TODO: just delegate to Projection operator?
  /*
   * TODO: do we need to stay within the Dataset monad here? 
   *   yes, since this will be exposed in the DSL
   *   but we are already acting on a Variable
   *   only expose via Dataset API?
   */
  //TODO: support fully qualified names
//  def getVariableByName(name: String): Option[Variable] = {
//    val ds = Dataset(this).project(name) 
//    if (ds.variables.isEmpty) None
//    else Some(ds.variables(0))
//  }
//  def getVariableByName(name: String): Option[Variable] = {
//    if (this.name == name || this.hasAlias(name)) Some(this)
//    else this match {
//      case s: Scalar => None  //didn't match above
//      case t @ Tuple(vars) => {
//        //keep tuple level, consider namespace
//        Some(Tuple(vars.flatMap(_.getVariableByName(name))))
//        //TODO: deal with empty Tuple?
//        //TODO: deal with data defined in tuple
//      }
//      case f @ Function(domain, range) => {
//        domain.getVariableByName(name) match {
//          case Some(v) => {
//            //assume 1D domain, for now
//            //TODO: make Index Function, make sure it gets Data 
//            //  only if the Function does not have the data
//            //  otherwise, need a Function wrapper
//            //TODO: compare to Projection
//            ???
//            //TODO: multi-dimensional, must be product set to extract just those values, or generalize by repeating for all domain samples?
//          }
//          case None => range.getVariableByName(name) match {
//            //TODO: deal with Data defined in function
//            case Some(v) => Some(Function(domain, v))
//            case None => None
//          }
//        }
//      }
//    }
//  }
  
  
  def containsVariable(name: String): Boolean = this match {
    case s: Scalar => if (s.name == name) true else false
    case Tuple(vars) => vars.exists(_.containsVariable(name))
    case Function(domain, range) => domain.containsVariable(name) || range.containsVariable(name)
  }
  
  def hasAlias(alias: String): Boolean = {
    metadata.get("alias") match {
      case Some(s) => s.split(",").contains(alias)
      case None => false
    }
  }
  
  //replaced by toSeq
//  /**
//   * Return a Seq of all the Scalar Variables (i.e. leafs) in this Variable.
//   */
//  def getScalars: Seq[Scalar] = getScalars(this)
//  
//  private def getScalars(variable: Variable): Seq[Scalar] = {
//    //recursive accumulator
//    def acc(v: Variable, list: Seq[Scalar]): Seq[Scalar] = v match {
//      case null => list
//      case s: Scalar => acc(null, list :+ s)
//      case Tuple(vars) => vars.flatMap(getScalars(_))
//      case Function(d, r) => getScalars(d) ++ getScalars(r)
//    }
//    
//    acc(this, List())
//  }
  
  
  //TODO: filter?
  //TODO: project?
  //  or should logic be encapsulated in Operation
  //  consider scala collections, they have filter...
  
 // def iterator
  
  /**
   * Get a Data Iterator for this Variable.
   * If it doesn't contain data, gather it from the kids.
   */
  def dataIterator: Iterator[Data] = {
    if (data.notEmpty) data.iterator
    else this match {
      case Tuple(vars) => concatData(vars) 
      case Function(d, r) => concatData(Seq(d,r))
      case s: Scalar => s.data.iterator
    }
  }

  /**
   * Concatenate the Data Iterators of the given Seq of Variables
   * into a single Data Iterator.
   */
  private def concatData(vars: Seq[Variable]): Iterator[Data] = new Iterator[Data] {
    val its: Seq[Iterator[Data]] = vars.map(_.dataIterator)
    def hasNext = its.head.hasNext //assume all have the same length
    def next = {
      val ds: Seq[Data] = its.map(_.next)
      Data(ds)(Data.empty) //hack to disambiguate after type erasure
    }
  }
  
  
  override def equals(that: Any): Boolean = (this, that) match {
    case (v1: Scalar, v2: Scalar) => v1._metadata == v2._metadata && v1._data == v2._data
    
    case (v1: Tuple, v2: Tuple) => v1._metadata == v2._metadata && v1._data == v2._data && 
      v1.variables == v2.variables 
    
    //iterate through all samples
    case (v1: Function, v2: Function) => v1._metadata == v2._metadata && 
      (v1.iterator zip v2.iterator).forall(p => p._1 == p._2)
      //TODO: confirm same length? iterate once issues
    
    case _ => false
  }
  
  //TODO: override def hashCode
  
  
  override def toString() = this match { //name +": "+ data.toString
  //use "unknown" only for scalars
    case s: Scalar => s.name
    case t: Tuple => if (t.name == "unknown") t.variables.mkString("(", ", ", ")")
      else t.name + ": " + t.variables.mkString("(", ", ", ")")
    case f: Function => f.domain.toString + " -> " + f.range.toString
  }
   
//--- Units ---
  //TODO: applicable to Scalar, Numbers only?
//  /**
//   * Get the "units" metadata as an object that we can use for unit conversion...
//   */
//  def getUnits(): UnitOfMeasure = {
//    metadata.get("units") match {
//      case Some(u) => UnitOfMeasure(u)
//      case None => null
//    }
//  }
//  
//  /**
//   * Convert this Variable to another unit of measure.
//   */
//  def convert(to: UnitOfMeasure): Variable = {
//    val converter = UnitConverter(getUnits(), to)
//    converter.convert(this)
//  }
  
  /*
   * consider traits
   * new Scalar with Data?
   *   but trait can't have state
   *   might still be useful instead of saying "if data != null"
   *   would allow us to call data methods directly on vars
   *     instead of v.data.x
   *   but in what context would we want to use Vars in place of Data?
   *   we probably shouldn't expose Data apis
   *   still need Var relationships to do math
   * with Metadata, same args
   */
}

object Variable {
  //TODO: factory methods, mixin math,...
  
  //Use Variable(md, data) in pattern match to expose metadata and data.
  //Subclasses may want to expose data values
  def unapply(v: Variable) = Some((v.metadata, v.data))
}