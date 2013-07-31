package latis.ops.math

abstract class BinOp extends Function2[Double, Double, Double]

//TODO: see Numeric.Ops

case class Add      extends BinOp {def apply(a: Double, b: Double): Double = a + b}
case class Subtract extends BinOp {def apply(a: Double, b: Double): Double = a - b}
case class Multiply extends BinOp {def apply(a: Double, b: Double): Double = a * b}
case class Divide   extends BinOp {def apply(a: Double, b: Double): Double = a / b}
case class Modulo   extends BinOp {def apply(a: Double, b: Double): Double = a % b}
case class Power    extends BinOp {def apply(a: Double, b: Double): Double = Math.pow(a, b)}

object BinOp {
  val ADD      = Add()
  val SUBTRACT = Subtract()
  val MULTIPLY = Multiply()
  val DIVIDE   = Divide()
  val MODULO   = Modulo()
  val POWER    = Power()
}
