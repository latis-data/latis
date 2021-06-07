package latis.ops

import scala.util._

import requests._

import latis.dm._

/**
 * Removes any sample with a url variable that fails a HEAD request.
 */
class UrlValidator extends Operation {

  override def applyToSample(sample: Sample): Option[Sample] = {
    sample.findVariableByName("url") match {
      case Some(Text(url)) =>
        Try(requests.head(url).statusCode) match {
          case Success(200) => Some(sample)
          case _ => None
        }
    }
  }
}

object UrlValidator extends OperationFactory {

  override def apply(): UrlValidator = new UrlValidator()
}
