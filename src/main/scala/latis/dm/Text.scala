package latis.dm

class Text(value: String) extends Scalar[String](value)

object Text {
  def apply(value: String) = new Text(value)
}