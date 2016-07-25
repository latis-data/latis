package latis.ops;

import latis.dm.Dataset
import latis.dm.Function
import latis.dm.Sample

/**
 * Given a Dataset of the form:
 *   index -> (time, version, ...)
 * with multiple "version"s for the same "time"
 * keep only the max version and return the dataset as:
 *   time -> (...)
 */
class TakeMaxVersion extends Operation {
	  
	override def apply(dataset: Dataset): Dataset = {
    val samples = dataset.groupBy("time").sorted match {  //make sorted function of time
      case Dataset(Function(it)) => it.toList.map(sample => {
        //for each time, make a function of version and keep the max 
        sample match {
          case Sample(time, f) => {
            val ds2 = Dataset(f).groupBy("version").sorted.last //sample with max version
            val range = ds2 match {
              //(index -> index -> range)
              case Dataset(Function(it)) => it.next match {
                case Sample(_, Function(it)) => it.next match {
                  case Sample(_, range) => range
                }
              }
            }
            Sample(time, range)
          }
        }
      })
    }
    //TODO: update metadata
    val md = dataset.getMetadata
    Dataset(Function(samples), md)
  }
	  
}

object TakeMaxVersion extends OperationFactory {
	  
  override def apply(): TakeMaxVersion = new TakeMaxVersion()
	  
  override def apply(args: Seq[String]): TakeMaxVersion = new TakeMaxVersion()
	  
}