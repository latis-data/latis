package latis.reader.tsml.agg

import java.io.File

import scala.collection.mutable.ArrayBuffer

import latis.dm.Dataset
import latis.dm.Function
import latis.dm.Sample
import latis.dm.Scalar
import latis.dm.Text
import latis.dm.Tuple
import latis.dm.TupleMatch
import latis.ops.Operation
import latis.ops.agg.Join
import latis.ops.agg.TileJoin
import latis.ops.filter.FirstFilter
import latis.ops.filter.LastFilter
import latis.ops.filter.Selection
import latis.reader.tsml.TsmlAdapter
import latis.reader.tsml.TsmlReader
import latis.reader.tsml.ml.Tsml
import latis.ops.filter.NearestNeighborFilter

/**
 * A TsmlAdapter that reads data from each file in a file list 
 * and joins the results. The tsml should be structured like so: 
 * 
 * <dataset>
 *   <adapter class="latis.reader.tsml.agg.FileJoinAdapter"
 *            join="<class that implements latis.ops.agg.Join>"/>
 *   
 *   <dataset>
 *     ...
 *     This dataset returns the list of files/URLs to be read
 *     ...
 *   </dataset>
 *   
 *   <dataset>
 *     ...
 *     Defines how to read one granule (file/URL). It will be used as a template
 *     for each granule in the above dataset with the "location" updated.
 *     ...
 *   </dataset>
 *   
 * </dataset>
 *     
 */
class FileJoinAdapter(tsml: Tsml) extends TsmlAdapter(tsml) {
  
  /**
   * Dynamically construct the join operation.
   */
  lazy val joinOperation: Join = {
    getProperty("join") match {
      case Some(class_name) => {
        //dynamically construct join operation
        val cls = Class.forName(class_name)
        val ctor = cls.getConstructor()
        ctor.newInstance().asInstanceOf[Join]
        //TODO: error handling
      }
      case None => TileJoin() //default
    }
  }
  
  /**
   * Construct an adapter for the file list dataset from the first "dataset"
   * definition in the tsml.
   */
  lazy val fileListAdapter = TsmlAdapter(Tsml((tsml.xml \ "dataset").head))
  //TODO: allow any reader; need to be able to ask if it has a variable by name without reading
  
  /**
   * Tsml for the template to be used to read each file.
   */
  val templateTsml = Tsml((tsml.xml \ "dataset").last)
  
  /**
   * Operations to be passed to getDataset for file list.
   */
  val fileListOps = ArrayBuffer[Operation]()
  
  /**
   * Operations to be passed to getDataset for each granule Dataset.
   */
  val granuleOps = ArrayBuffer[Operation]()
  

  /** Function that determines which granules are read when joining. */
  private var fileListFilter: Option[Iterator[Sample] => Iterator[Sample]] = None

  /**
   * Manage how operations will be applied (e.g. to file list, granules and/or 
   * joined data). Selections (including NearestNeighbor) will be offered for 
   * the template Adapter to handle for each granule. Selections not handled, 
   * though applicable (granule has the parameter name), will be applied to the
   * joined data (later) by returning "false" here. Selections will be applied 
   * to the file list dataset if applicable. First and Last filters will always
   * be applied to the file list but not to granules.
   */
  override def handleOperation(op: Operation): Boolean = op match {
    case Selection(name, _, _) => 
      if (fileListHasName(name)) fileListOps += op //apply to file list dataset
      handleGranuleOp(name, op)
    
    //Apply first and last operations to both the file list and the joined files.
    //Note the return of "false" to tell getDataset to apply them to the result.
    case ff: FirstFilter => fileListOps += ff; false
    case lf: LastFilter => fileListOps += lf; false
    
    case NearestNeighborFilter(name, value) =>
      // The file list will be filtered to include only the granules
      // that the nearest sample might be in when filtering on the
      // domain variable of the file list.
      //
      // We are assuming a single nearest neighbor filter on the
      // domain variable of the file list (typically time). If
      // multiple filters are given, the first one handled here will
      // be the one applied to the file list. The others may still be
      // applied in other places.
      if (outerDomainHasName(fileListAdapter, name) && fileListFilter.isEmpty) {
        fileListFilter = Option(makeFileListFilter(value))
      }

      val handled = handleGranuleOp(name, op)
      // If this is for the outer domain, also apply to the joined dataset
      val adapter = TsmlAdapter(templateTsml)
      if (outerDomainHasName(adapter, name)) false else handled
   
    case _ => false //apply any other ops to the joined data
  }

  /**
   * Creates a filter selecting the set of granules required for a
   * join with a nearest neighbor filter applied.
   */
  private def makeFileListFilter(
    value: String
  ): Iterator[Sample] => Iterator[Sample] = { it =>
    // Pair each granule in the file list with the bounds of the bin
    // it represents, silently dropping samples that do not have
    // bounds. This simplifies the logic later.
    val withBounds: Iterator[(Sample, (Scalar, Scalar))] =
      it.flatMap(s => fileListBounds(s).map((s, _)))

    // If the file list is empty, we'll just keep it empty. Otherwise,
    // we will fold over the iterator to find the granules nearest to
    // our search value.
    if (! withBounds.hasNext) Iterator.empty else withBounds.next() match {
      case first @ (s, (b1, _)) =>
        if (b1.compare(value) > 0) {
          // If the search value falls before the start of the first
          // granule, we're already done.
          Iterator(s)
        } else {
          // The following fold over the iterator will construct a new
          // file list (an iterator of samples) containing only the
          // granules we need to read in order to find the sample
          // nearest our search value.
          //
          // The type of 'z' is what we're building in this fold:
          // Either[(Sample, (Scalar, Scalar)), (Sample, Sample)]
          //
          // Recall that Either[A, B] means that something is A or B.
          //
          // This type is useful because we have two cases to consider
          // during the fold:
          //
          // 1. We haven't found the search value yet.
          //
          // 2. The search value is bounded by two granules.
          //
          // The Right type (a pair of Samples) covers case two. Once
          // we've hit this case we no longer need to look for the
          // search value, so we can just return the accumulator for
          // the rest of the fold.
          //
          // The Left type (a pair of a Sample and a pair of Scalars)
          // covers case one. This type is used to carry the last
          // sample we looked at so we can determine whether it and
          // the current sample cover the search value. If we reach
          // the end of the fold and we're still carrying the Left
          // type, that means the search value falls after the last
          // granule.
          val z: Either[(Sample, (Scalar, Scalar)), (Sample, Sample)] =
            Left(first)

          withBounds.foldLeft(z) {
            case (acc @ Right(_), _) => acc
            case (Left((s1, (b1, _))), cur @ (s2, (_, b2))) =>
              if (b1.compare(value) <= 0 && b2.compare(value) > 0) {
                // Our search value is either in s1 or s2, so return
                // both.
                Right((s1, s2))
              } else {
                // Our search value comes after the granules we've
                // looked at so far, so we need to continue the fold.
                Left(cur)
              }
          } match {
            case Left((s, _))    => Iterator(s)
            case Right((s1, s2)) => Iterator(s1, s2)
          }
        }
    }
  }

  /** Extracts bounds added to file list datasets by DomainBinner. */
  private def fileListBounds(s: Sample): Option[(Scalar, Scalar)] = s match {
    case Sample(_, Tuple(vs)) => vs.lastOption.flatMap {
      case TupleMatch(s: Scalar, e: Scalar) => Option((s, e))
      case _ => None
    }
    case _ => None
  }

  /**
   * Does the file list dataset have a variable with the given name.
   */
  private def fileListHasName(name: String): Boolean = {
    fileListAdapter.getOrigDataset.findVariableByName(name).nonEmpty
  }
  
  /**
   * Does the domain Variable of the outer Function have the given name.
   */
  private def outerDomainHasName(adapter: TsmlAdapter, name: String): Boolean = {
    val hasName = adapter.getOrigDataset match {
      case Dataset(f: Function) => f.getDomain.hasName(name)
      case _ => false
    }
    try { adapter.close } catch { case e: Exception => }
    hasName
  }
  
  /**
   * Operations that are targeted for a specific Variable may be applicable to
   * the granule datasets. 
   */
  private def handleGranuleOp(name: String, op: Operation): Boolean = {
    val tmpAdapter = TsmlAdapter(templateTsml)
    val granHandled = tmpAdapter.handleOperation(op)
    val granHasName = tmpAdapter.getOrigDataset.findVariableByName(name).nonEmpty
    try { tmpAdapter.close } catch { case e: Exception => }

    if (granHandled) granuleOps += op

    // If the granule has the selected variable 
    // and it is not handled by the adapter
    // then we need to apply it later to the joined data 
    // so return "false" indicating that it has not yet been applied.
    !(granHasName && !granHandled)
  }
  
  /**
   * Keep the set of readers for each file so we can close them.
   */
  private val readers = ArrayBuffer[TsmlReader]()
  
  /**
   * Make a Dataset from each file in the file list Dataset by substituting the
   * file name into the "location" of the data tsml template.
   * Use the join Operation to combine them into one Dataset.
   */
  def joinDatasets: Dataset = {
    val fileDataset = fileListAdapter.getDataset(fileListOps)
    val dss: Iterator[Dataset] = fileListToGranules(fileDataset)
    
    //Make metadata for the joined dataset based on the tsml
    val md = makeMetadata(tsml.dataset)
    
    //Apply the join operation to the datasets
    if (dss.isEmpty) Dataset(null, md)
    else dss.reduceLeft(joinOperation(_,_)) match {
      case Dataset(v) => Dataset(v, md) //add metadata (e.g. dataset name) to the dataset
      case _ => Dataset(null, md)
    }
  }
  
  /** Creates a Dataset for each granule in a file list. */
  private def fileListToGranules(fs: Dataset): Iterator[Dataset] = fs match {
    case Dataset(Function(it)) =>
      //TODO: could get srcDir from tsml, but also needed for ZipWriter?
      //TODO: consider "baseURL"
      val dir = fs.getMetadata.get("srcDir").fold("")(_ + File.separator)

      val filter: Iterator[Sample] => Iterator[Sample] =
        fileListFilter.getOrElse(identity)

      filter(it).flatMap { file: Sample =>
        val path = file.findVariableByName("file").map {
          case Text(file) => dir + file
        }.orElse {
          file.findVariableByName("url").map {
            case Text(url) => url
          }
        }.getOrElse {
          throw new Exception(s"No 'file' or 'url' Variable found in Dataset '$fs'")
        }

        //make a reader with the file location plugged into the template tsml
        val reader = TsmlReader(templateTsml.dataset.setLocation(path))
        readers += reader //keep a copy so we can close later

        try {
          Option(reader.getDataset(granuleOps))
        } catch {
          case _: Exception => None //skip invalid files
        }
      }
    case _ => Iterator.empty
  }

  /**
   * Override to add logic to join the file datasets.
   */
  override def getDataset(ops: Seq[Operation]): Dataset = {
    //TODO: consider how PIs are handled
    //could we use the super getDataset = joinDatasets? 

    // Throw an error if the request exceeds a time range limit.
    validateTimeRange(ops)

    val otherOps = ops.filterNot(handleOperation(_))
    
    val ds = joinDatasets
    
    val ds2 = otherOps.foldLeft(ds)((dataset, op) => op(dataset))

    //Cache the dataset if requested
    //Note, this requires that the dataset id in the tsml matches the tsml file name
    getProperty("cache") match {
      case Some("memory") => ds2.cache
      case _ => ds2
    }
  }
  
  /**
   * Close the reader for each data file.
   * Since these don't get closed until the end of a LatisServer request,
   * there is risk of having too many files open.
   */
  def close: Unit = readers.foreach(_.close)

}
