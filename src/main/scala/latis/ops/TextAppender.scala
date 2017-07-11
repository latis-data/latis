package latis.ops

import latis.dm._
import latis.dm.Scalar
import latis.dm.Text
import latis.util.iterator.MappingIterator

class TextAppender(name: String, suffix: String) extends Operation() {
  
  /**
   * Append 'suffix' to any Text Variable named 'name'.
   */
  override def applyToScalar(s: Scalar): Option[Variable] = s.hasName(name) match {
    case true => {
      val t = s match {
        case Text(str) => str + suffix
        case _ => throw new UnsupportedOperationException("Can only append to a Text Variable.")
      }
      val md = s.getMetadata + ("length", t.length.toString)
      Some(Text(md, t))
    }
    case false => Some(s)
  }
  
  /**
   * Override to apply Operation to domain as well as range.
   */
  override def applyToSample(sample: Sample): Option[Sample] = {
    val od = applyToVariable(sample.domain)
    val or = applyToVariable(sample.range)
    (od, or) match {
      case (Some(d), Some(r)) => Some(Sample(d, r))
      case _ => None //this will never happen
    }
  }
  
}

object TextAppender extends OperationFactory {
  override def apply(args: Seq[String]): TextAppender = new TextAppender(args(0), args(1))
  
  def apply(name: String, suffix: String): TextAppender = new TextAppender(name, suffix)
}