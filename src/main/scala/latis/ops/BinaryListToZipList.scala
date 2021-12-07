package latis.ops

import latis.dm._
import latis.metadata.Metadata

/**
 * Convert a Dataset with a binary variable into a Dataset
 * specialized for writing to a zip file: zipEntry -> binary.
 */
class BinaryListToZipList extends Operation {
  
  var dsName = ""
  
  override def apply(dataset: Dataset): Dataset = {
    dsName = dataset.getName
    super.apply(dataset)
//    dataset match {
//      case ds @ DatasetSamples(it) =>
//        val samples = it.toList.flatMap(applyToSample(_))
//        Dataset(Function(samples), ds.getMetadata)
//      case _ => ??? //TODO: empty dataset?
//    }
  }

  /**
   * Turns the Sample into a zip entry of the form: dsName -> binary,
   * if it has a Binary Variable. 
   */
  override def applyToSample(sample: Sample): Option[Sample] = sample.getVariables.find {
    case Binary(_) => true
    case _ => false
  } match {
    case Some(bv) =>
      Some(Sample(Text(Metadata("zipEntry"), dsName), bv))
    case _ => throw new UnsupportedOperationException("No Binary variable found in Sample.")
  }

}
