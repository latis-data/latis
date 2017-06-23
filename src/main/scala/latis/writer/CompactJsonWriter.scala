package latis.writer

import latis.dm.Dataset
import latis.dm.Function
import latis.dm.Index
import latis.dm.Sample
import latis.dm.Scalar
import latis.dm.Tuple
import latis.dm.Variable
import latis.time.Time

/**
 * Return data as nested arrays, without metadata.
 * One inner array for each sample.
 * Time will be presented in native JavaScript time: milliseconds since 1970.
 * Handy for clients that just want the data (e.g. HighCharts).
 */
class CompactJsonWriter extends JsonWriter {

  override def makeHeader(dataset: Dataset): String = ""
  override def makeFooter(dataset: Dataset): String = ""
  
  override def makeLabel(variable: Variable): String = ""
    
  /**
   * Override to present time in native JavaScript units: milliseconds since 1970.
   */
  override def makeScalar(scalar: Scalar): String = {
    //quick fix to replace missing data with "null"
    //TODO: user should apply replace_missing(NaN) once that is working
    if (scalar.isMissing) "null"
    else scalar match {
      case t: Time => t.getJavaTime.toString  //use java time for "compact" json
      case _ => super.makeScalar(scalar)
    }
  }
  
  override def makeSample(sample: Sample): String = makeSample(sample, List[String]()) //invoke with no prefix
  
  //use optional pre to duplicate leading values when we have a nested function
  //One element for each preceding variable as a String      
  def makeSample(sample: Sample, pre: Seq[String]): String = {
    //break sample into domain and range components
    val Sample(d, r) = sample
    
    d match {
      case _: Index => varToString(r) //drop Index domain
      
      //handle case with nested function but no other range vars  
      //TODO: handle more general cases with scalars and tuples in the range with the function
      //TODO: handle 'flattened' option - one row per outer sample without inner domain values
      case _ => r match {
        case f: Function => {
          val pre = List(varToString(d)) //only the domain values are repeated, for now
          val lines = f.iterator.map(makeSample(_, pre))
          //lines.mkString("[", sys.props("line.separator"), "]") 
          val delim = "," + sys.props("line.separator")
          lines.mkString("", delim , "") 
        }
        case _ => {
          //varToString(Tuple(d.toSeq ++ r.toSeq)) //combine domain and range vars into one Tuple, just a way to get the "[]"?
          val vars = d.toSeq ++ r.toSeq
          val vs = vars.map(varToString(_))
          (pre ++ vs).mkString("[", ",", "]") 
        }
      }
    }
  }
    
  /**
   * Represent a tuple as an array with each element being an array of values.
   */
  override def makeTuple(tuple: Tuple): String = {
    tuple.getVariables.map(varToString(_)).mkString("[", ",", "]") 
  }
}
