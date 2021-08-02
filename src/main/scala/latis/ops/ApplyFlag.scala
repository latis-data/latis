package latis.ops

import latis.data.Data
import latis.dm._
import latis.ops.filter.Selection

/**
 * Adds a new variable that makes a "flagged" copy of the given variable.
 *
 * If the predicate (in the form of a selection on another variable)
 * is false, the copy will have the same value. Otherwise, this replaces
 * the value with its fill value.
 */
class ApplyFlag(vname: String, predicate: String) extends Operation {

  /** Defines a selection operation to apply the predicate. */
  private lazy val selection = Selection(predicate)

  override def applyToSample(sample: Sample): Option[Sample] = {
    // Get scalar to be flagged
    val scalar: Scalar = sample.findVariableByName(vname) match {
      case Some(s: Scalar) => s
      case _ => throw new RuntimeException(s"Variable not found: $vname")
    }

    // Make flagged scalar
    val newScalar: Scalar = scalar.updatedMetadata("name" -> s"${vname}_flagged")

    // Make sample with flagged scalar
    val newSample = if (selection.applyToSample(sample).isEmpty) {
      // predicate false, leave data alone
      appendToRange(sample, newScalar)
    } else {
      // predicate true, replace with fill value
      //TODO: add fill/missing value to metadata if not already defined
      appendToRange(sample, newScalar(Data(scalar.getFillValue)))
    }

    Some(newSample)
  }

  /** Appends the given Scalar to the range of the given Sample. */
  private def appendToRange(sample: Sample, variable: Variable): Sample = {
    val range: Tuple = Tuple(sample.range.toSeq :+ variable)
    Sample(sample.domain, range)
  }
}

object ApplyFlag extends OperationFactory {

  override def apply(args: Seq[String]): Operation = args.toList match {
    case vname :: predicate :: Nil => new ApplyFlag(vname, predicate)
    case _ => throw new RuntimeException("ApplyFlag requires 2 arguments: vname, predicate")
  }
}
