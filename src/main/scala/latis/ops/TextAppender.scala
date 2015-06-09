package latis.ops

import latis.dm.Sample
import latis.dm.Scalar
import latis.dm.Text

class TextAppender(name: String, suffix: String) extends Operation() {
  
  override def applyToScalar(s: Scalar) = s.getName match {
    case this.name => {
      val t = s.getValue.toString + suffix
      val md = s.getMetadata + ("length", t.length.toString)
      Some(Text(md, t))
    }
    case _ => Some(s)
  }
  
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
  override def apply(args: Seq[String]) = new TextAppender(args(0), args(1))
  
  def apply(name: String, suffix: String) = new TextAppender(name, suffix)
}