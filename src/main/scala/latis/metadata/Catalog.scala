package latis.metadata
import latis.util.LatisProperties
import java.net.URL
import java.io.File

/*
 * TODO:
 * Map a dataset name/id to a URL for a Reader
 * "type" maps to Reader?
 *   tsml => TsmlReader
 *   jdbc => DatabaseReader
 * support multiple types, see ncml
 * use url protocol? tsml:...
 *   URI
 *   but how to distinguish file from http...?
 *  
 * 
 * Define an interface/trait that can be impld by THREDDS,...
 *   getAccessUrl(dsname) ?
 *   optional "type" arg?
 *   
 * Could the catalog be represented as a LaTiS Dataset?
 * just like any other metadata, RDF
 * 
 * catalog probably doesn't belong in core
 * with core one manually makes a Reader with a url
 * server needs other mechanism
 * maybe a TimeSeriesReader could be another project that offers an API with dsname
 * same with writers and filters
 * so core needs no properties
 * 
 * Consider JNDI resource lookup.
 * 
 * Generalize "catalog" to "registry"?
 * DatasetRegistry?
 * this would be useful in the core, too
 * But "registry" implies a "register" operation.
 */

object Catalog {
  
  def getTsmlUrl(dataset: String): URL = {
    val dsdir = LatisProperties.getOrElse("dataset.dir", "datasets")
    val tsml = dsdir + File.separator + dataset + ".tsml"
    
    //If tsml path has URL scheme, assume we have a valid url  //TODO: use URI api?
    if (tsml.contains(":")) new URL(tsml)
    //or absolute file path, prepend 'file' scheme
    else if (tsml.startsWith(File.separator)) new URL("file:" + tsml)
    //otherwise try to resolve relative path
    else new URL("file:" + LatisProperties.resolvePath(tsml))
  }
}