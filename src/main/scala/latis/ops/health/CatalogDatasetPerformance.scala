package latis.ops.health

import com.typesafe.scalalogging.LazyLogging
import latis.dm.Dataset
import latis.dm.Function
import latis.dm.Sample
import latis.dm.Text
import latis.dm.Tuple
import latis.metadata.Metadata
import latis.ops.Operation
import latis.ops.OperationFactory
import latis.ops.filter.FirstFilter
import latis.reader.DatasetAccessor
import scala.collection.mutable.ArrayBuffer
import latis.ops.filter.LastFilter
import latis.ops.filter.Selection
import latis.time._
import java.util.Calendar
import java.util.Date
import java.util.TimeZone
import java.text._

/* 
 * Given a catalog dataset following the DCAT ontology, describe 
 * basic performance statistics from each dataset such as 
 * access time and memory usage. 
 */
class CatalogDatasetPerformance extends Operation with LazyLogging {
  
  var ssiSingleTimeDs: Option[Dataset] = None //Used to optimize finding a valid wavelength in large SSI datasets
  
  //Hack to account for large datasets that don't have "ssi" in their names
  //TODO: Rework this entire CatalogDatasetPerfromance class using metadata from LEMR
  //      because really, we need more info about datasets than just their names to do the right things.
  //      (WEBAPP-993)
  val ssiNames: List[String] = List("musil_see", "musil_sim")
  val oneDayNames: List[String] = List("sdo_eve_bands_l2b", "sdo_eve_diodes_l2b", "sdo_eve_lines_l2b")
 
  /**
   * Apply this Operation to the given Dataset
   * (overriden to change the dataset's name).
   */
  override def apply(dataset: Dataset): Dataset = dataset match { 
    case Dataset(variable) => {
      val md = dataset.getMetadata
      applyToVariable(variable) match {
        case Some(v) => Dataset(v, md + ("name", "dataset_performance"))
        case None => Dataset.empty
      }
    }
    case _ => dataset
  }
  
  /*
   * Acquire performance statistics from the datasets listed in 
   * this catalog.
   * Test time and wavelength separately for SSI datasets.
   */
  override def applyToFunction(function: Function): Option[Function] = {
    function match {
      case Function(it) => {
        val samples = ArrayBuffer[Sample]()
        it.foreach { s => s match {
          //TODO: Consider matching sample's range as well (to validate catalog's formatting)
          case Sample(Text(name), _) if name contains "ssi" => {
            val timeSeriesResults = getPerformanceResults(name, " (time series)") //" (time series)" gets added to ds name in logs
            samples += makeSampleFromResults(name+" (time series)", timeSeriesResults._1, timeSeriesResults._2, timeSeriesResults._3) 
            
            val spectralResults = getPerformanceResults(name, " (spectrum)") //" (spectrum)" gets added to ds name in logs
            samples += makeSampleFromResults(name+" (spectrum)", spectralResults._1, spectralResults._2, spectralResults._3)
            ssiSingleTimeDs = None //clear the shortened global ds since we're moving on to another ds now
          }
          //TODO: Consider matching sample's range as well (to validate catalog's formatting)
          case Sample(Text(name), _) => {
            val results = getPerformanceResults(name)
            samples += makeSampleFromResults(name, results._1, results._2, results._3)
          }
          //Case only matches when sample's domain is not Text 
          case _ => samples += makeSampleFromResults("[INVALID RECORD]", "N/A", "N/A", "N/A")
          }   
        }
        Some(Function(samples.toSeq)) 
      }
    }
  }
  
  def makeSampleFromResults(n: String, a: String, t: Any, m: Any): Sample = {
    Sample(Text(Metadata("ds_name"), n), 
      Tuple(Seq(Text(Metadata("alive"), a),
        Text(Metadata("access_time"), t),
        Text(Metadata("memory_usage"), m))))
  }
  
  /*
   * Given a dataset name (and optionally an operation descriptor), 
   * assess the performance of that dataset and return a tuple of 
   * the results (whether the ds is alive, how many seconds it took 
   * to read the first sample, and how much memory it used).  
   * This is all done concurrently to avoid duplicate operations. 
   * 
   * Resulting tuple takes the form: (alive, time, memUsage) 
   */
  def getPerformanceResults(name: String, op: String = ""): (String, Double, Double) = {
    System.gc //Run the garbage collector to *greatly* improve accuracy of mem usage.
              //This does not impart a noticable performance hit here.
    
    val startTime = System.nanoTime
    val startFreeMem = Runtime.getRuntime.freeMemory
    
    val isAlive = requestWholeDataset(name, op)
    
    val endFreeMem = Runtime.getRuntime.freeMemory
    val endTime = System.nanoTime
    val timeDiff = (endTime - startTime)/1e9 //converts nanoseconds to seconds
    val memUsed = (startFreeMem - endFreeMem)/1e6 //converts bytes to megabytes                  
   
    logger.info("PERFORMANCE: dsName=" + name+op + ", alive=" + isAlive + ", timeUsed(s)=" + timeDiff + ", memoryUsed(MB)=" + memUsed + " ")
                         
    (isAlive.toString, timeDiff, memUsed)
  }
  
  /*
   * Return true once a dataset of a given name has served all its data.
   * If a dataset fails to load, return false. 
   * Only load a single time and a single wavelength from SSI datasets.
   */    
  def requestWholeDataset(name: String, op: String): Boolean = {
    var reader: DatasetAccessor = null //needed to "close" after the read
    try {
      op match {
        case " (time series)" => {
          reader = DatasetAccessor.fromName(name)
          val dsTime = reader.getDataset(Seq(LastFilter())).force
          dsTime match {
            case Dataset(Function(it)) if it.hasNext => {
              ssiSingleTimeDs = Some(dsTime)
              true
            }
            case _ => false
          }
        }
        case " (spectrum)" | _ if ssiNames.contains(name) => { 
          //try to be clever and get a wavelength from ssiSingleTimeDs, which is dramatically smaller, to select on
          ssiSingleTimeDs match {
            case Some(dsSSI) => {
              try {
                val selection = Seq(Selection("wavelength~" + getFirstWavelength(dsSSI))) 
                reader = DatasetAccessor.fromName(name)
                val dsWavelength = reader.getDataset(selection).force
                dsWavelength match {
                  case Dataset(Function(it)) => it.hasNext
                  case _ => false
                }
              } catch {
                case e: Throwable => {
                  //Try selecting on "energy" instead of "wavelength" before giving up
                  val selection = Seq(Selection("energy~" + getFirstEnergy(dsSSI))) 
                  reader = DatasetAccessor.fromName(name)
                  val dsEnergy = reader.getDataset(selection).force
                  dsEnergy match {
                    case Dataset(Function(it)) => it.hasNext
                    case _ => false
                  }
                }
              }
            }
            case None => { 
              //TODO: Find a way to avoid guessing a wavelength/energy and/or decide if there's a better one than 121.5
              //TODO: Maybe warn in logs that a wavelength value was hard coded?
              try {
                reader = DatasetAccessor.fromName(name)
                val dsWavelength = reader.getDataset(Seq(Selection("wavelength~121.5"))).force
                dsWavelength match {
                  case Dataset(Function(it)) => it.hasNext
                  case _ => false
                }
              } catch {
                case e: Throwable => {
                  //Try selecting on "energy" instead of "wavelength" before giving up
                  reader = DatasetAccessor.fromName(name)
                  val dsEnergy = reader.getDataset(Seq(Selection("energy~121.5"))).force
                  dsEnergy match {
                    case Dataset(Function(it)) => it.hasNext
                    case _ => false
                  }
                }
              }
            }
          }
        }
        case _ if oneDayNames.contains(name) => {
          val (day1, day2): (String, String) = getFirstAndSecondDays(name)
          val ops = List(Selection("time >= "+day1), Selection("time < "+day2)) //TODO: consider if doing time<[two days after] is better than time<=[one day after], given that the latter seems to return only one sample sometimes
          
          val ds = DatasetAccessor.fromName(name).getDataset(ops).force
          ds match {
            case Dataset(Function(it)) => it.hasNext
            case _ => false
          }
        }
        case _ => {
          val ds = DatasetAccessor.fromName(name).getDataset.force
          ds match {
            case Dataset(Function(it)) => it.hasNext
            case _ => false
          }
        }
      }
    } catch {
      case e: Throwable => 
        logger.info("Dataset access failed for " + name + ". Unable to read from LaTiS: " + e.getMessage); false
    } finally {
      try { reader.close } catch { case _: Exception => } //try to close, but if we can't, don't complain
    }
  }
  
  /*
   * Given the name of a dataset, return the ISO-formatted date Strings
   * of the first and second day in it.
   */
  def getFirstAndSecondDays(name: String): (String, String) = {
    val day1: Time = DatasetAccessor.fromName(name).getDataset(Seq(FirstFilter())) match {
      case Dataset(Function(it)) => it.next match {
        case Sample(t: Time, _) => t
      }
    }
    val day1ISO = day1.toIso + 'Z'
  
    val c: Calendar = Calendar.getInstance();
    c.setTime(day1.toDate);
    c.add(Calendar.DATE, 2); //same as c.add(Calendar.DAY_OF_MONTH, 2); //TODO: consider if doing time<[two days after] is better than time<=[one day after], given that the latter seems to return only one sample sometimes
    val day2: Date = c.getTime();
    val tz = TimeZone.getTimeZone("UTC");
    val df: DateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"); 
    df.setTimeZone(tz);
    
    val day2ISO = df.format(day2);

    (day1ISO, day2ISO)
  }
  
  /*
   * Given a SSI dataset of the form (time -> wavelength -> ssi),
   * return the first value stored for wavelength.
   */
  def getFirstWavelength(dsSSI: Dataset): Double = {
    dsSSI match {
      case Dataset(Function(it)) => it.next match {
        case Sample(d, r) => { //r should be: wavelength -> ssi
          val wavelength = r.findAllVariablesByName("wavelength")(0).getNumberData.doubleValue
          wavelength
        }
      }
    }
  }
  
  /*
   * Given a SSI dataset of the form (time -> energy -> ssi),
   * return the first value stored for wavelength.
   */
  def getFirstEnergy(dsSSI: Dataset): Double = {
    dsSSI match {
      case Dataset(Function(it)) => it.next match {
        case Sample(d, r) => { //r should be: energy -> ssi
          val energy = r.findAllVariablesByName("energy")(0).getNumberData.doubleValue
          energy
        }
      }
    }
  }
  
  
}

object CatalogDatasetPerformance extends OperationFactory {
  override def apply(): CatalogDatasetPerformance = new CatalogDatasetPerformance
}