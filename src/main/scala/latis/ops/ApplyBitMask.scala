package latis.ops

import latis.data.Data
import latis.dm._

/**
 * Adds a new variable that makes a "masked" copy of the given variable.
 *
 * If the mask, in the form of a bitwise AND on another variable,
 * is zero, the copy will have the same value. Otherwise, this replaces
 * the value with its fill value.
 */
class ApplyBitMask(vname: String, mask: String) extends Operation {

  /** Extracts the mask variable and bitmask from the given mask expression. */
  private lazy val (maskVar, maskVal): (String, Int) = mask.split("""\s*&\s*""").toList match {
    case a :: b :: Nil => (a, b.toInt)
    case _ =>
      val msg = "Invalid mask. Expecting 'myVar&#' where '#' is the bit mask as an integer."
      throw new RuntimeException(msg)
  }

  override def applyToSample(sample: Sample): Option[Sample] = {
    // Get scalar to be masked
    val scalar: Scalar = sample.findVariableByName(vname) match {
      case Some(s: Scalar) => s
      case _ => throw new RuntimeException(s"Variable not found: $vname")
    }

    // Get the scalar with the bits
    val maskScalar: Scalar = sample.findVariableByName(maskVar) match {
      case Some(s: Scalar) => s
      case _ => throw new RuntimeException(s"Variable not found: $maskVar")
    }

    // Make masked scalar
    val newScalar: Scalar = scalar.updatedMetadata("name" -> s"${vname}_masked")

    // Make sample with masked scalar
    val newSample = if (applyMask(maskScalar, maskVal)) {
      // some bits match mask, replace with fill value
      //TODO: add fill/missing value to metadata if not already defined
      appendToRange(sample, newScalar(Data(scalar.getFillValue)))
    } else {
      // no bits match mask, leave data alone
      appendToRange(sample, newScalar)
    }

    Some(newSample)
  }

  /** Returns false if any 1-bit in the scalar aligns with a 1-bit in the bitmask. */
  private def applyMask(scalar: Scalar, bitmask: Int): Boolean = scalar match {
    case Integer(v) => (bitmask & v) != 0
    case _ => throw new RuntimeException(s"Invalid mask variable: $maskVar")
  }

  /** Appends the given Scalar to the range of the given Sample. */
  private def appendToRange(sample: Sample, variable: Variable): Sample = {
    val range: Tuple = Tuple(sample.range.toSeq :+ variable)
    Sample(sample.domain, range)
  }
}

object ApplyBitMask extends OperationFactory {

  override def apply(args: Seq[String]): Operation = args.toList match {
    case vname :: mask :: Nil => new ApplyBitMask(vname, mask)
    case _ => throw new RuntimeException("ApplyBitMask requires 2 arguments: vname, mask")
  }
}
