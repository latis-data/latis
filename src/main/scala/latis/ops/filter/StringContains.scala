package latis.ops.filter

import latis.dm.Text
import latis.ops.OperationFactory

/**
 * Select values of 'name' that contain 'value'.
 */
class StringContains(name: String, value: String) extends Selection(name, "=~", value) {
  
  override def applyToText(text: Text): Option[Text] = {
    if (text.hasName(vname)) text match {
      case Text(text_value) if (text_value.contains(value)) => Some(text) //keep match
      case _ => None
    } else Some(text) //operation doesn't apply to this Scalar Variable, no-op
  }
  
}

object StringContains extends OperationFactory {
  
  def apply(name: String, value: String) = new StringContains(name, value)
  
  override def apply(args: Seq[String]) = StringContains(args(0), args.tail.mkString(","))
  
  def unapply(sc: StringContains): Option[(String, String)] = Some((sc.vname, sc.value))
  
}