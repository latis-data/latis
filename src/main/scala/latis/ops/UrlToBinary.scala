package latis.ops

import latis.data.Data
import latis.dm._
import latis.metadata.Metadata

/**
 * Operation to replace a URL variable with its contents in a Binary variable.
 *
 * The resulting binary variable will be named "image" and it will include the
 * "mediaType" metadata property from the HTTP response's "content-type" header.
 * If mediaType is already defined for the url variable, it will override the
 * returned content-type. If neither are defined, this will set the mediaType to
 * "application/octet-stream".
 */
class UrlToBinary extends Operation {

  override def applyToScalar(scalar: Scalar): Option[Variable] = scalar.getName match {
    case "url" => scalar match {
      case Text(url) =>
        val r = requests.get(url)
        val mtype = scalar.getMetadata("mediaType")
          .orElse(r.httpContentType)
          .getOrElse("application/octet-stream")
        val md = Metadata(
          "name" -> "image",
          "mediaType" -> mtype
        )
        Some(Binary(md, Data(r.bytes)))
      case _ => throw new RuntimeException("UrlToBinary expects url variable to be of type text.")
    }
    case s => Some(scalar) //no-op for other variables
  }

}

object UrlToBinary extends OperationFactory {
  override def apply(): Operation = new UrlToBinary
}
