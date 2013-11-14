package latis.reader.tsml

import latis.reader.tsml.ml.Tsml
import scala.collection._
import java.util.Hashtable
import javax.naming._
import javax.naming.directory._

class LdapAdapter(tsml: Tsml) extends GranuleAdapter(tsml) {

  def location: String = getProperty("location") match {
    case Some(v) => v
    case None => throw new RuntimeException("LdapAdapter must have 'location' defined.")
  }
  
  def group: String = getProperty("group") match {
    case Some(v) => v
    case None => throw new RuntimeException("LdapAdapter must have 'group' defined.")
  }
  
  lazy val context = {
    // Set up the environment for creating the initial context
    val env = new Hashtable[String, Object](2);
    env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory")
    env.put(Context.PROVIDER_URL, location)
    new InitialDirContext(env)
  }
  
  def executeQuery: Iterator[SearchResult] = {
    // Specify the ids of the attributes to return (Dataset Variables)
    //TODO: use projection
    val attIDs = variableNames.toArray
    
    // Add search attributes.
    //TODO: generalize to use selections
    val matchAttrs = new BasicAttributes(true) // ignore case
    matchAttrs.put(new BasicAttribute("memberof")) //include every entry that has a memberof?
    
    // Specify the group to match, if it is defined
    getProperty("group") match {
      case Some(group) => matchAttrs.put(new BasicAttribute("memberof", group))
      case None => 
    }
    
    
    // Make the query.
    val answer = context.search("ou=People", matchAttrs, attIDs)  //java Enumeration
    JavaConversions.enumerationAsScalaIterator(answer)            //scala Iterator
    //TODO: test for null, return empty iterator, or will conversion handle it?
  }
  
  //TODO: consider using Record semantics, but dangerous to depend on toString format
//  override def recordIterator: Iterator[Record] = executeQuery.map(result => List(result.toString))
//    //return each result as space delimited values and super will do the rest
//    //Just use toString for now: uid=sroughto: null:null:{mail=mail: steve.roughton@lasp.colorado.edu, uid=uid: sroughto, cn=cn: Steve Roughton}
//
//  override def parseRecord(record: Record): Map[Name, Value] = {
//    val map = mutable.HashMap[Name, Value]()
//    ???
//  }
  
  
  def readData: immutable.Map[String, immutable.Seq[String]] = {
    val map = initDataMap
    
    //Get the query results
    val results = executeQuery
    
    // Parse results, one for each person found. Build up array of values for each variable.
    for (result <- results) {
      //Get the attributes for the person. Should be same as attIDs = Dataset Variables.
      val atts = result.getAttributes
      for (vname <- variableNames) {
        //Note, the value of an attribute is more attributes. Join with ",".
        val value = JavaConversions.enumerationAsScalaIterator(atts.get(vname).getAll).mkString(",")
        map(vname) append value
      }
    }
    
    //return as immutable dataMap
    immutableDataMap(map)
  }
  
  override def close {context.close}
}