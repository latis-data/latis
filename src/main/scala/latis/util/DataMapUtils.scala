package latis.util

import latis.data.Data
import latis.dm.Function
import latis.dm.Sample
import latis.dm.Scalar
import latis.dm.Tuple
import latis.dm.Variable
import latis.dm.Index

/**
 * Utilities for getting Variables out of a Data map.
 */
object DataMapUtils {
    
  /**
   * Start point for reading from an Iterator of Data maps.
   */
  def dataMapsToFunction(dms: Iterator[Map[String,Data]], function: Function): Function = {
    val mapsPerSample = innerLength(function.getSample)
    mapsPerSample match {
      case 1 => Function(function, dms.map(dm => dataMapsToSample(Seq(dm), function.getSample)))
      case _ => {
        val it = dms.grouped(mapsPerSample).map(dmg => dataMapsToSample(dmg, function.getSample))
        Function(function, it)
      }
    }
  }
  
  def dataMapsToSample(dms: Seq[Map[String,Data]], s: Sample): Sample = {
    Sample(dataMapsToVar(dms, s.domain), dataMapsToVar(dms, s.range))
  }
  
  def dataMapsToTuple(dms: Seq[Map[String,Data]], t: Tuple): Tuple = {
    Tuple(t.getVariables.map(dataMapsToVar(dms, _)), t.getMetadata)
  }
  
  def dataMapToScalar(dm: Map[String,Data], s: Scalar): Scalar = s match {
    case i: Index => i
    case _ => {
      val data = dm(s.getName)
      s(data).asInstanceOf[Scalar]
    }
  }
  
  /**
   * Create a Variable v with the data in dms.
   */
  def dataMapsToVar(dms: Seq[Map[String,Data]], v: Variable): Variable = v match {
    case s: Scalar => dataMapToScalar(dms.head, s)
    case s: Sample => dataMapsToSample(dms, s)
    case t: Tuple => dataMapsToTuple(dms, t)
    case f: Function => dataMapsToFunction(dms.iterator, f)
  }
  
  def innerLength(s: Sample): Int = s.range.findFunction match {
    case None => 1
    case Some(f) => f.getLength * innerLength(f.getSample)
  }
}