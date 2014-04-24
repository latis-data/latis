package latis.reader.tsml

import latis.data.Data
import latis.dm.Text
import latis.reader.tsml.ml.Tsml
import latis.util.StringUtils

import java.util.Hashtable

import scala.collection.Iterator
import scala.collection.JavaConversions
import scala.collection.Map
import scala.collection.mutable

import javax.naming.Context
import javax.naming.directory.BasicAttribute
import javax.naming.directory.BasicAttributes
import javax.naming.directory.InitialDirContext
import javax.naming.directory.SearchResult

class LdapAdapter(tsml: Tsml) extends IterativeAdapter[SearchResult](tsml) {
  
  def getRecordIterator: Iterator[SearchResult] = executeQuery

  def parseRecord(record: SearchResult): Option[Map[String,Data]] = {
    //val valueMap = mutable.Map[String,ArrayBuffer[String]]()
    val dataMap = mutable.Map[String,Data]()
    
    val atts = record.getAttributes
    for (v <- getOrigScalars) {
      val vname = v.getName
      //Note, the value of an attribute is more attributes. Join with ",".
      val value = JavaConversions.enumerationAsScalaIterator(atts.get(vname).getAll).mkString(",")
      //valueMap(vname) append value
      val data = Data(StringUtils.padOrTruncate(value, v.asInstanceOf[Text].length)) //enforce length
      dataMap += (vname -> data)
    }
    
    Some(dataMap)
  }
  
//  /**
//   * Read 
//   */
//  def init {
//    val dataMap = readData
//    
//  }
  
//  def readData: Map[String, DataSeq] = {
//    val map = mutable.Map[String,ArrayBuffer[String]]()
//    
//    //Get the query results
//    val results = executeQuery
//    
//    // Parse results, one for each person found. Build up array of values for each variable.
//    for (result <- results) {
//      //Get the attributes for the person. Should be same as attIDs = Dataset Variables.
//      val atts = result.getAttributes
//      for (vname <- getOrigScalarNames) {
//        //Note, the value of an attribute is more attributes. Join with ",".
//        val value = JavaConversions.enumerationAsScalaIterator(atts.get(vname).getAll).mkString(",")
//        map(vname) append value
//      }
//    }
//    
//    //convert tmp string values to Data
//    //assume all variables are type Text, deal with length
//    val dataMap = mutable.Map[String,Data]()
//    
//    for (scalar <- getOrigScalars) {
//      val name = scalar.getName
//      val buffer = map(name)
//      //TODO: if length = 0?          
//      val length: Int = scalar.getMetadata("length") match {
//        case Some(l) => l.toInt
//        case None => buffer.map(_.length).max
//      }
//      val data = Data(buffer, length)
//      dataMap += (name -> data)
//    }
//    
//    dataMap
//  }
  
  
  
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
    val attIDs = getOrigScalarNames.toArray
    
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
  
  override def close {context.close}
}