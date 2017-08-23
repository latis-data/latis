package latis.reader

import latis.dm.Dataset
import latis.dm.Function
import latis.dm.Real
import latis.dm.Sample
import latis.dm.Text
import latis.metadata.Metadata
import latis.ops.Operation
import scala.collection.mutable.ArrayBuffer
import latis.ops.filter.Selection
import latis.reader.tsml.TsmlReader
import latis.dm.TupleMatch
import latis.dm.Tuple

/**
 * Generates a catalog of the datasets that LEMR currently serves to the lisird website.
 * Datasets are obtained via a Sparql query to LEMR itself.
 */
class LisirdLemrCatalogGenerator extends DatasetAccessor {
  
  override def getDataset: Dataset = generateCatalog
  
  def getDataset(operations: Seq[Operation]): Dataset = {
    val dataset = generateCatalog
    operations.foldLeft(dataset)((ds,op) => op(ds))
  }
  
  def generateCatalog: Dataset = {
    val samples = getLisirdDatasetsFromLemr.map(name => Sample(Text(Metadata("name"), name),
                                                                 Tuple(Text(Metadata("description"), ""),
                                                                 Tuple(Text(Metadata("accessURL"), name), 
                                                                 Metadata("distribution")))))
    val f = Function(samples, Metadata("lisird_datasets"))
    val dataset = Dataset(f, Metadata("LEMRCatalog"))
    dataset                                                     
  }
  
  def getLisirdDatasetsFromLemr: List[String] = {
    val lisirdDatasets = ArrayBuffer[String]()
    
    val ops = ArrayBuffer[Operation]()
    ops += Selection("predicate", "=~", ".*(identifier$|lisirdRender$)") //looking for predicates with "identifier" or "lisirdRender"
    val lemrDs = DatasetAccessor.fromName("lemr_datasets").getDataset(ops) //the Sparql query is defined in this tsml
    
    lemrDs match {
      case Dataset(Function(it)) if it.hasNext => {
        var sample = it.next 
        var nextSample: Sample = null
        while (it.hasNext) {  
          sample.range match {
            case TupleMatch(Text(s), Text(p), Text(name)) if p contains "identifier" => {
              nextSample = it.next
              nextSample.range match {
                case TupleMatch(Text(s2), Text(p2), Text(lisirdRender)) => {
                  if (s2.equals(s) && p2.contains("lisirdRender") && lisirdRender.equalsIgnoreCase("true")) {
                    lisirdDatasets += name //the dataset represented by this "subject" is rendered in LISIRD, so include its identifier 
                    sample = it.next 
                  } else { //"nextSample" might have a dataset identifier, so don't skip it
                      sample = nextSample
                  }
                } 
              }
            }
            case _ => sample = it.next //keep looking for a dataset identifier
          }
        }
      }
      case _ => throw new RuntimeException("No data returned from LEMR.")
    }
    
    lisirdDatasets.distinct.toList.sorted
  }
  
  def close: Unit = {}
  
}


object LisirdLemrCatalogGenerator {
  def apply(): LisirdLemrCatalogGenerator = new LisirdLemrCatalogGenerator
}
