package latis.ops

import latis.data.Data
import latis.dm.Function
import latis.dm.Index
import latis.dm.Number
import latis.dm.Sample
import latis.dm.Text
import latis.dm.Tuple
import latis.dm.Variable
import latis.dm.implicits.variableToDataset
import latis.time.Time

/**
 * Reduces a Function to a single Sample. The value of Number Variables
 * will be the sum of the value of that Variable in all Samples of the original Function.
 * The value of the domain and Text Variables will be their last values from the original Function.
 * Not supported for nested Functions. 
 */
class Sum extends Operation {
  
  override def applyToFunction(f: Function): Option[Variable] = {
    var vars: Seq[Variable] = f.toSeq.map(_ match {
      case n: Number => n(Data(0))
      case t: Text => t
    })
    val it = f.iterator
    
    it.foreach(s => vars = vars.zip(s.toSeq).map(p => p._2 match {
      case s.domain => p._2
      case t: Text => p._2
      case n: Number => (p._1 + p._2).unwrap
    }))
    
    //Metadata is lost during addition, so re-apply the new Data to the original Variables
    val nvars = f.toSeq.zip(vars).map(p => p._1 match {
      case n: Number => p._1(p._2.getData)
      case t: Text => p._2
    })
    
    nvars.length match {
      case 0 => None
      case 1 => Some(Function(f, Iterator(Sample(Index(), nvars.head))))
      case 2 => Some(Function(f, Iterator(Sample(nvars.head, nvars.last))))
      case _ => Some(Function(f, Iterator(Sample(nvars.head, Tuple(nvars.tail)))))
    }
  }

}

object Sum {
  def apply() = new Sum
}

