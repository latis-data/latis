package latis.ops

import java.util.Base64

import latis.data.Data
import latis.dm._

/**
 * Replace binary variables with a base64 encoding of the bytes.
 *
 * This will update the mediaType metadata if defined.
 */
class Base64Encoder extends Operation {

  /**
   * Replaces a Binary variable with a Text variable with a base64 encoded string.
   */
  override def applyToScalar(scalar: Scalar): Option[Variable] = scalar match {
    case Binary(bytes) =>
      val data = Data(Base64.getEncoder().encodeToString(bytes))
      val md = scalar.getMetadata("mediaType") match {
        case Some(mt) => scalar.getMetadata() + ("mediaType" -> s"$mt;base64")
        case None     => scalar.getMetadata()
      }
      Some(Text(md, data))
    case s => Some(s)
  }
}

object Base64Encoder extends OperationFactory {
  override def apply(): Base64Encoder = new Base64Encoder
}
