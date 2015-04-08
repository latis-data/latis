package latis.dm

import latis.data.Data
import latis.data.EmptyData
import latis.metadata.Metadata
import latis.metadata.EmptyMetadata

import scala.Option.option2Iterable
import scala.collection.Seq

/**
 * Implementation for much of what Variables need to do.
 */
abstract class AbstractVariable(val metadata: Metadata = EmptyMetadata, val data: Data = EmptyData) extends Variable {
  
  def getMetadata(): Metadata = metadata
  def getData: Data = data
  
  def getName = name
  private lazy val name = metadata.get("name") match {
    case Some(s) => s
    case None => this match {
      case _: Function => "samples" //default name for Functions
      case _: Sample => "sample"    //default name for Samples
      case _ => "unknown" //TODO: generate unique name?
    }
  }
  
  /**
   * Length of the Variable depending on type.
   *   Scalar: 1
   *   Tuple: number of variable members
   *   Function: number of samples
   */
  /*
   * TODO: is 'length' too overloaded?
   * should Text return length of string?
   * Tuple: getElementCount, Arity?
   * Function: getSampleCount
   */
//  def getLength: Int = this match {
//    case _: Scalar => 1  //TODO: consider Scalars with SeqData, implicit IndexFunction
//    case Tuple(vars) => vars.length
//    case f: Function => getMetadata.get("length") match {
//      case Some(l) => l.toInt
////      case None => f.iterator.length //may be costly, problem with IterableOnce? try looking at data?
//      //TODO: need to account for nested Functions? maybe Dataset should but not Function?
//      //  this does fail for nested Function which is not backed by iterable data
////TODO: error or unknown = -1?
//    }
//  }
  
  /**
   * Size of this Variable in bytes.
   */
  def getSize: Int = size
  private lazy val size: Int  = this match {
    case _: Index => 4 
    case _: Real => 8 //double
    case _: Integer => 8 //long
    case t: Text => t.length * 2 //2 bytes per char  //TODO: avoid confusing t.length with getLength
    case _: Binary => getMetadata("length") match {
      case Some(l) => l.toInt + 8 //account for the termination mark
      //NOTE: potential for trouble: termination marker not required,
      //and number of bytes written by BinaryWriter won't match size since it doesn't write the marker
      case None => throw new Error("Must declare length of Binary Variable.")
    } 
    
    case Tuple(vars) => vars.foldLeft(0)(_ + _.getSize)
    case f: Function => {
      val length = f.getLength
      if (length < 0) throw new Error("Can't get size of Function with undefined length: " + f)
      else length * (f.getDomain.getSize + f.getRange.getSize)
    }
  }
  
  /**
   * Return this Variable as a "flattened" Seq of Scalars.
   * In other words, considering the Variable as a tree,
   * return all the leaves in depth first order.
   * Note, no attempt is made to juggle any data, for now,
   * so we might end up with a mess for complex datasets.
   */
  def toSeq: Seq[Scalar] = this match {
    case s: Scalar => Seq(s)
    case Tuple(vars) => vars.foldLeft(Seq[Scalar]())(_ ++ _.toSeq)
    case f: Function => f.getDomain.toSeq ++ f.getRange.toSeq
  }
  
  
  /**
   * Find the first Variable within this Variable by the given name or alias.
   */
  def findVariableByName(name: String): Option[Variable] = {
    if (hasName(name)) Some(this)
    else this match {
      case _: Scalar => None  //didn't match above
      case Tuple(vars) => {
        val matchingVars = vars.flatMap(_.findVariableByName(name))
        matchingVars.length match {
          case 0 => None
          case _ => Some(matchingVars.head)
          //case n: Int => throw new RuntimeException("Found " + n + " variables that match the name: " + name)
        }
      }
      case f: Function => Sample(f.getDomain, f.getRange).findVariableByName(name) //delegate to Tuple
    }
  }

  /**
   * Return the first Function found in this variable, or
   * None if no functions are contained
   */
  def findFunction: Option[Function] = this match {
    case _: Scalar => None
    case Tuple(vars) => {
      vars.view.flatMap(_.findFunction).headOption
    }
    case f: Function => Some(f)
  }
  
  /**
   * Does this Variable have the given name or alias.
   */
  def hasName(name: String): Boolean = {
    //TODO: consider long, qualified name
    //TODO: support wildcard, regex?
    getName == name || hasAlias(name)
  }
  
  /**
   * Does this Variable have an alias that matches the given name.
   */
  def hasAlias(alias: String): Boolean = {
    metadata.get("alias") match {
      case Some(s) => s.split(",").contains(alias)
      case None => false
    }
  }
  
  //probably not a good idea to test equality for large variables
  override def equals(that: Any): Boolean = (this, that) match {
    case(r1: Real, r2: Real) => ((r1.doubleValue.isNaN && r2.doubleValue.isNaN) || r1.getData == r2.getData) && r1.getMetadata == r2.getMetadata 
  
    case (v1: Scalar, v2: Scalar) => v1.getMetadata == v2.getMetadata && v1.getData == v2.getData
    
    case (v1: Tuple, v2: Tuple) => v1.getMetadata == v2.getMetadata && v1.getData == v2.getData && 
      v1.getVariables == v2.getVariables 
    
    //iterate through all samples
    case (v1: Function, v2: Function) => v1.getMetadata == v2.getMetadata &&
      v1.iterator.corresponds(v2.iterator)(_==_)
      //(v1.iterator zip v2.iterator).forall(p => p._1 == p._2)
      //TODO: confirm same length? iterate once issues
    
    case _ => false
  }
  
  //TODO: override def hashCode
  
  
  override def toString() = this match { 
    case s: Scalar => s.name
    case t: Tuple => if (t.name == "unknown") t.getVariables.mkString("(", ", ", ")")
      else t.name + ": " + t.getVariables.mkString("(", ", ", ")")
    case f: Function => f.getDomain.toString + " -> " + f.getRange.toString
  }

}
