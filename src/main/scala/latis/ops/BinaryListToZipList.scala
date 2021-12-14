package latis.ops

import latis.dm._
import latis.metadata.Metadata
import latis.time.Time

/**
 * Convert a Dataset with a binary variable into a Dataset 
 * specialized for writing to a zip file: zipEntry -> binary.
 * 
 * Zip entry names will be the name of the dataset with 
 * domain values appended for disambiguation.
 */
class BinaryListToZipList extends Operation {
  
  private var dsName = ""
  
  override def apply(dataset: Dataset): Dataset = {
    dsName = dataset.getName
    super.apply(dataset)
  }

  /**
   * Turns the Sample into a zip entry of the form: zipEntry -> binary,
   * if it has a Binary Variable. 
   */
  override def applyToSample(sample: Sample): Option[Sample] = sample.toSeq.find(_.isInstanceOf[Binary]) match {
    case Some(bv) =>
      Some(Sample(Text(Metadata("zipEntry"), disambiguate(dsName, sample.domain)), bv))
    case _ => throw new UnsupportedOperationException("No Binary variable found in sample")
  }

  /**
   * Appends "_d" to zip entry names, where "d" is the stringified form of the given domain value.
   */
  private def disambiguate(name: String, domain: Variable): String = name + "_" + domainToString(domain)

  /**
   * Returns a String representation of a domain variable
   * that is acceptable to disambiguate a zip entry.
   */
  private def domainToString(domain: Variable): String = domain match {
    case time: Time => time match {
      case Text(t)   => t //keep formatting
      case _: Number => time.toIso //TODO: consider stripping special characters like '-' and ':'
    }
    case tup: Tuple => tup.getVariables.map(domainToString).mkString("_")
    case d: Scalar  => d.stringValue
    case _ => throw new UnsupportedOperationException(s"Unsupported data type for domain variable '$domain'")
  }
}
