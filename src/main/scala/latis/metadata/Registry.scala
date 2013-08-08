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
 */

object Registry {

  def getTsmlUrl(dataset: String): URL = {
    //TODO: get dataset.dir from properties
    //  need absolute path for now, complications with LatisProperties singleton not knowing about server
    //  need to use servlet context to resolve relative path
    
    //val url = "file:" + LatisProperties.resolvePath("datasets/" + dataset + ".tsml")
    var base = LatisProperties.getOrElse("dataset.dir", "datasets")
    if (! base.startsWith(File.separator)) base = LatisProperties.resolvePath(base)
    val url = "file:" + base + File.separator + dataset + ".tsml"
    new URL(url)
  }
}