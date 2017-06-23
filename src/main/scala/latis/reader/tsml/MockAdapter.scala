package latis.reader.tsml

import latis.reader.tsml.ml.Tsml
import latis.dm._
import latis.util.DataMapUtils
import latis.data.seq.DataSeq
import latis.data.Data
import java.util.NoSuchElementException
import latis.data.set.IndexSet
import latis.data.value.DoubleValue
import latis.data.value.LongValue

/**
 * Constructs a Dataset and creates Data for each Variable based on a "mock_type"
 * attribute defined in the tsml. Currently supported "mock_type"s are:
 *   "INDEX":  fills data with a sequence starting at 0 and increasing by 1.
 *             (this is applied by default if no "mock_type" is specified)
 *   "RANDOM": fills data with random Double values in the interval [0.0, 1.0).
 */
class MockAdapter(tsml: Tsml) extends TsmlAdapter(tsml) {
  
  def close: Unit = {}
  
  /**
   * Put Data into the cache
   */
  override def init: Unit = {
    val names = getOrigScalarNames
    val v = getOrigDataset match {
      case Dataset(v) => v
      case _ => null
    }
    val lengths = dataLengths(v,1)
    
    val values = names.map(name => getValues(name, lengths(name)))
    val data = getOrigScalars.zip(values).map(p => p._1 match {
      case i: Integer => DataSeq(p._2.map(v => LongValue(v.toInt)))
      case r: Real => DataSeq(p._2.map(v => DoubleValue(v)))
      case t: Text => ??? //cannot mock text yet
    })
    cache(names.zip(data).toMap)
  }
  
  /**
   * How many data values are needed for each variable 
   */
  def dataLengths(v: Variable, i: Int): Map[String, Int] = v match {
    case s: Scalar => Map(s.getName -> i)
    case t: Tuple => t.getVariables.map(dataLengths(_, i)).reduceLeft(_ ++ _)
    case f: Function => {
      val dl = f.getMetadata("length").getOrElse(throw new 
          Exception("Functions must have a defined length to be mocked.")).toInt
      dataLengths(f.getDomain, dl) ++ dataLengths(f.getRange, dl * i)
    }
  }  
  
  /**
   * Get a list of values that will be mapped to Data for the given variable.
   */
  def getValues(name: String, length: Int): Seq[Double] = {
    val mockType = try tsml.getVariableAttribute(name, "mock_type")
    catch {
      case e: NoSuchElementException => "INDEX" //default to index
    }
    
    mockType match {
      case "INDEX" => getIndexValues(length)
      case "RANDOM" => getRandomValues(length)
    }
  }
  
  def getIndexValues(length: Int): Seq[Double] = {
    Seq.range(0,length).map(_.toDouble)
  }
  
  def getRandomValues(length: Int): Seq[Double] = {
    Seq.fill(length)(math.random)
  }
    
}
