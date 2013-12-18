package latis.dm

import latis.data._
import latis.metadata._
import latis.ops.math.BasicMath
import latis.time.Time
import scala.collection._

/*
 * TODO: 2013-10-21
 * traits: Scalar, Real, Integer,..., Tuple, Function
 * each could have "this: Variable =>"  but not IS-A Variable
 * when to we need to pattern match on Variable vs type?
 * do we need a Variable trait? yes, write to interfaces
 * and a VariableImpl
 */

trait Variable {
  def getMetadata: Metadata
  def getData: Data
  
  def getName: String
  
  def isNumeric: Boolean = getData.isInstanceOf[NumberData]
  def getNumberData: NumberData = getData.asInstanceOf[NumberData]
  //TODO: deal with TextData
  
  def getLength: Int
  def getSize: Int
  
  def getVariableByName(name: String): Option[Variable]
  def hasName(name: String): Boolean
  
  def toSeq: Seq[Scalar]
  def getDataIterator: Iterator[Data]
  
  def getVariables: Seq[Variable] //TODO: immutable.Seq?
  def apply(index: Int): Variable = getVariables(index)
  
  //TODO: toStringValue?
}

abstract class Variable2(val metadata: Metadata = EmptyMetadata, val data: Data = EmptyData) extends Variable {
  
  def getMetadata: Metadata = metadata
  def getData: Data = data
  
  def getName = name
  lazy val name = metadata.get("name") match {
    case Some(s) => s
    case None => "unknown" //TODO: generate unique name?
  }
  
  /**
   * Length of the Variable depending on type.
   *   Scalar: 1
   *   Tuple: number of variable members
   *   Function: number of samples
   */
  def getLength: Int = this match {
    case _: Scalar => 1  //TODO: consider Scalars with SeqData, implicit IndexFunction
    case Tuple(vars) => vars.length
    case f: Function => getMetadata.get("length") match {
      case Some(l) => l.toInt
      case None => f.iterator.length //may be costly, try looking at data?
    }
  }
  
  /**
   * Size of this Variable in bytes.
   */
  def getSize: Int = size
  lazy val size: Int  = this match {
    case _: Index => 4
    case _: Real => 8 //TODO: override for Scalars to support custom?
    case _: Integer => 8 //long
    case t: Text => t.length * 2 //2 bytes per char  //TODO: consider Seq, get max
    case b: Binary => b.length
    
    case Tuple(vars) => vars.foldLeft(0)(_ + _.getSize)
    case f @ Function(d,r) => f.getLength * (d.getSize + r.getSize)
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
    case Function(d,r) => d.getSize + r.getSize 
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
  
  /**
   * Return this Variable as a Seq of Variables.
   */
  def getVariables: Seq[Variable] = this match {
    case s: Scalar => Seq(s)
    //case Sample(d,r) => d.getVariables ++ r.getVariables  more like flatten
    //case tup: Tuple => tup.
    //TODO: Function?
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
  
  
  /*
   * TODO: do we need to stay within the Dataset monad here? 
   *   this will be exposed in the DSL
   *   but we are already acting on a Variable
   *   only expose via Dataset API?
   *   Dataset API can override to return a Dataset, 
   *   the DSL should not expose anything but Dataset
   *   use this for internal use
   */
  //TODO: support fully qualified names
  //TODO: "findVariableByName"? all descendants like netcdf, "get" for direct kids?
  //TODO: stop as soon as first is found, like Seq.find
  //Get the actual Variable object with the given name or alias.
  //Don't try to maintain the entire model structure.
  def getVariableByName(name: String): Option[Variable] = {
    if (hasName(name)) Some(this)
    else this match {
      case _: Scalar => None  //didn't match above
      case Tuple(vars) => {
        val matchingVars = vars.flatMap(_.getVariableByName(name))
        matchingVars.length match {
          case 0 => None
          case 1 => Some(matchingVars.head)
          case n: Int => throw new RuntimeException("Found " + n + " variables that match the name: " + name)
            //TODO: warn and return first?
        }
      }
      case Function(domain, range) => Sample(domain, range).getVariableByName(name) //delegate to Tuple
    }
  }

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
  
  
//  def containsVariable(name: String): Boolean = this match {
//    //TODO: support alias
//    case s: Scalar => if (s.name == name) true else false
//    case Tuple(vars) => vars.exists(_.containsVariable(name))
//    case Function(domain, range) => domain.containsVariable(name) || range.containsVariable(name)
//  }
  
  //include alias
  //TODO: consider long, qualified name
  def hasName(name: String): Boolean = {
    //TODO: support wildcard, regex?
    getName == name || hasAlias(name)
  }
  
  def hasAlias(alias: String): Boolean = {
    metadata.get("alias") match {
      case Some(s) => s.split(",").contains(alias)
      case None => false
    }
  }
  
  
  //TODO: filter?
  //TODO: project?
  //  or should logic be encapsulated in Operation
  //  consider scala collections, they have filter...
  
 // def iterator
  
  /**
   * Get a Data Iterator for this Variable.
   * If it doesn't contain data, gather it from the kids.
   */
  def getDataIterator: Iterator[Data] = {
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
    val its: Seq[Iterator[Data]] = vars.map(_.getDataIterator)
    def hasNext = its.head.hasNext //assume all Variables have the same number of samples
    def next = {
      val ds: Seq[Data] = its.map(_.next)
      Data(ds)(Data.empty) //hack to disambiguate after type erasure
    }
  }
  
  
  override def equals(that: Any): Boolean = (this, that) match {
    case (v1: Scalar, v2: Scalar) => v1.getMetadata == v2.getMetadata && v1.getData == v2.getData
    
    case (v1: Tuple, v2: Tuple) => v1.getMetadata == v2.getMetadata && v1.getData == v2.getData && 
      v1.getVariables == v2.getVariables 
    
    //iterate through all samples
    case (v1: Function, v2: Function) => v1.getMetadata == v2.getMetadata && 
      (v1.iterator zip v2.iterator).forall(p => p._1 == p._2)
      //TODO: confirm same length? iterate once issues
    
    case _ => false
  }
  
  //TODO: override def hashCode
  
  
  override def toString() = this match { //name +": "+ data.toString
  //use "unknown" only for scalars
    case s: Scalar => s.name
    case t: Tuple => if (t.name == "unknown") t.getVariables.mkString("(", ", ", ")")
      else t.name + ": " + t.getVariables.mkString("(", ", ", ")")
    case f: Function => f.getDomain.toString + " -> " + f.getRange.toString
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
  def unapply(v: Variable) = Some((v.getMetadata, v.getData))
}