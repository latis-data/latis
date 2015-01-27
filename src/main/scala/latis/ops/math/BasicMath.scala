package latis.ops.math

import latis.dm.Dataset

import BinOp.ADD
import BinOp.AND
import BinOp.DIVIDE
import BinOp.LT
import BinOp.MODULO
import BinOp.MULTIPLY
import BinOp.POWER
import BinOp.SUBTRACT

trait BasicMath { this: Dataset =>
  
  //Define binary operation symbols that can be used on a Dataset.
  def + (that: Dataset): Dataset  = MathOperation(ADD, that)(this)
  def - (that: Dataset): Dataset  = MathOperation(SUBTRACT, that)(this)
  def * (that: Dataset): Dataset  = MathOperation(MULTIPLY, that)(this)
  def / (that: Dataset): Dataset  = MathOperation(DIVIDE, that)(this)
  def % (that: Dataset): Dataset  = MathOperation(MODULO, that)(this)
  def ** (that: Dataset): Dataset = MathOperation(POWER, that)(this) //WARNING: has same operator precedence as multiplication
  def < (that: Dataset): Dataset =  MathOperation(LT, that)(this)
  def && (that: Dataset): Dataset = MathOperation(AND, that)(this)
  
}
