package latis.ops

import latis.dm._
import latis.metadata.Metadata

/**
 * Convert a Dataset with a binary variable into a Dataset 
 * specialized for writing to a zip file: domain -> (dsName, binary).
 */
class BinaryListToZipList extends Operation {
  
  private var dsName = ""
  
  override def apply(dataset: Dataset): Dataset = {
    dsName = dataset.getName
    super.apply(dataset)
  }

  /**
   * Turns the Sample into a zip entry of the form: domain -> (dsName, binary),
   * if it has a Binary Variable. 
   */
  override def applyToSample(sample: Sample): Option[Sample] = sample match {
    case Sample(domain, _) => sample.getVariables.find { //TODO: major bug: doesn't look inside tuples
      case Binary(_) => true
      case _ => false
    } match {
      case Some(bv) =>
        Some(Sample(domain, Tuple(List(Text(Metadata("zipEntry"), dsName), bv))))
      case _ => throw new UnsupportedOperationException("No Binary variable found in sample")
    }
    case _ => throw new UnsupportedOperationException("Invalid sample")
  }

}
