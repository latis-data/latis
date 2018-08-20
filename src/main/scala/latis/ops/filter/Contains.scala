package latis.ops.filter

import com.typesafe.scalalogging.LazyLogging
import latis.dm.Dataset
import latis.dm.Sample
import latis.dm.Scalar
import latis.dm.Tuple

/**
 * Filter based on a constraint expression of the form
 * "a = {1,2,3}"
 */
class Contains(val vname: String, val values: Seq[String]) extends Filter with LazyLogging {

  override def apply(ds: Dataset): Dataset = ds match {
    case Dataset(v) => ds.findVariableByName(vname) match {
      case None => throw new UnsupportedOperationException(s"Dataset does not contain unknown variable '$vname'")
      case _ => super.apply(ds)
    }
    case _ => ds
  }

  override def applyToScalar(scalar: Scalar): Option[Scalar] = {
    try {
      scalar match {
        case s: Scalar => if (scalar.hasName(vname)) {
          if (values.exists { v =>
            //swallow exceptions thrown by impossible comparisons to avoid short circuiting the "exists" search
            val cmp: Int = try { scalar.compare(v) } catch { case _: NumberFormatException => -1 }
            cmp == 0
          }) Some(scalar) else None
        } else Some(scalar) //operation doesn't apply to this Scalar Variable, no-op
      }
    } catch {
      case e: Exception => {
        logger.warn("Contains filter threw an exception: " + e.getMessage)
        None
      }
    }
  }

  override def applyToSample(sample: Sample): Option[Sample] = {
    val x = sample.getVariables.map(applyToVariable(_))
    x.find(_.isEmpty) match {
      case Some(_) => None //found an invalid variable, exclude the entire sample
      case None => Some(Sample(x(0).get, x(1).get))
    }
  }

  override def applyToTuple(tuple: Tuple): Option[Tuple] = {
    val x = tuple.getVariables.map(applyToVariable(_))
    x.find(_.isEmpty) match {
      case Some(_) => None //found an invalid variable, exclude the entire tuple
      case None => Some(Tuple(x.map(_.get), tuple.getMetadata))
    }
  }

  override def toString: String = values.mkString(s"$vname={", ",", "}")

}

//TODO: companion object (LATIS-726)