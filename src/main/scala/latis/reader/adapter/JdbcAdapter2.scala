package latis.reader.adapter

import java.nio.ByteBuffer
import java.sql.Connection
import java.sql.DriverManager
import java.sql.ResultSet
import java.sql.Statement
import java.util.Calendar
import java.util.TimeZone

import scala.Option.option2Iterable
import scala.collection._
//import scala.collection.Seq
//import scala.collection.mutable
//import scala.collection.mutable.ArrayBuffer

import com.typesafe.scalalogging.LazyLogging

import javax.naming.InitialContext
import javax.naming.NameNotFoundException
import javax.sql.DataSource
import latis.data.Data
import latis.dm.Binary
import latis.dm.Dataset
import latis.dm.Function
import latis.dm.Index
import latis.dm.Integer
import latis.dm.Real
import latis.dm.Scalar
import latis.dm.Text
import latis.dm.Variable
import latis.ops.Operation
import latis.ops.Projection
import latis.ops.RenameOperation
import latis.ops.filter._
import latis.ops.filter.LastFilter
import latis.ops.filter.LimitFilter
import latis.ops.filter.Selection
import latis.reader.tsml.ml.Tsml
import latis.time.Time
import latis.time.TimeFormat
import latis.time.TimeScale
import latis.util.DataUtils
import latis.util.StringUtils
import latis.dm.Tuple
import latis.dm._
import scala.collection.mutable.ArrayBuffer
import latis.reader.adapter.IterativeAdapter3
import java.lang.Integer
import latis.ops.filter.TakeOperation

/**
 * Adapter for databases that support JDBC.
 * Assumes one row per record.
 */
class JdbcAdapter2(model: Model, properties: Map[String, String]) 
  extends IterativeAdapter3[JdbcAdapter2.JdbcRecord](model, properties)
  with LazyLogging {
  //TODO: catch exceptions and close connections
  //TODO: make model from ResultSetMetaData to make a Reader

  def getRecordIterator: Iterator[JdbcAdapter2.JdbcRecord] = getProperty("limit") match {
    case Some(lim) if (lim.toInt == 0) => new JdbcAdapter2.JdbcEmptyIterator()
    case _ => new JdbcAdapter2.JdbcRecordIterator(resultSet)
  }
  
  /**
   * Parse the data based on the Variable type (and the database column type, for time).
   */
  def parseRecord(record: JdbcAdapter2.JdbcRecord): Option[Map[String, Data]] = {
    val pairs: Seq[(String,Data)] = model.getScalars.map { vt =>
      val name = getVariableName(vt) //accounts for rename
      //Special handling for time; use name/alias for now
      val optData = if (vt.hasName("time")) parseTime(record, vt)
      else vt.getType match {
//TODO: need to skip index earlier        case "index"   => None
        case "integer" => parseInteger(record, vt)
        case "real"    => parseReal(record, vt)
        case "text"    => parseText(record, vt)
        case "binary"  => parseBinary(record, vt)
        case _ => ??? //TODO: error, unknown type
      }
      optData match {
        case Some(d) => (name, d)
        case None    => ??? //TODO: (name, makeFillData(vt))
      }
    }
    Option(pairs.toMap)
  }

  protected def parseInteger(record: JdbcAdapter2.JdbcRecord, vt: VariableType): Option[Data] = {
    val name = getVariableName(vt) //accounts for rename
    record.getLong(name).map(Data(_))
  }

  protected def parseReal(record: JdbcAdapter2.JdbcRecord, vt: VariableType): Option[Data] = {
    val name = getVariableName(vt) //accounts for rename
    record.getDouble(name).map(Data(_))
  }
    
  protected def parseText(record: JdbcAdapter2.JdbcRecord, vt: VariableType): Option[Data] = {
    val name = getVariableName(vt) //accounts for rename
    record.getString(name) match {
      case Some(v) =>
        //pad to max length, default to 4 characters
        //TODO: don't need consistent length if this Text is part of a simple record
        val length = vt.getMetadata("length") match { 
          case Some(s) => s.toInt
          case None => 4
        }
        Option(Data(StringUtils.padOrTruncate(v, length)))
      case None => None
    }
  }
    
  protected def parseBinary(record: JdbcAdapter2.JdbcRecord, vt: VariableType): Option[Data] = {
    val name = getVariableName(vt) //accounts for rename
    record.getBytes(name) map { v =>
      val length = vt.getMetadata("length") match { 
        case Some(s) => s.toInt
        case None => ??? //TODO: error? default?
      }
      val bytes = if (v.length > length) {
        val msg = s"JdbcAdapter found ${v.length} bytes which is longer than the max size: ${length}. The data will be truncated."
        logger.warn(msg)
        v.take(length) //truncate so we don't get buffer overflow
      } else v
      
      //TODO: do we need to enforce consistent length? consider poda use case
      //TODO: do we need the nullMark?
      val bb = ByteBuffer.allocate(length) //allocate a ByteBuffer for the max length
      bb.put(bytes) //add the data
      bb.put(DataUtils.nullMark) //add termination mark
      //Set the "limit" to the end of the data and rewind the position to the start.
      //Note, the capacity will remain at the max length.
      bb.flip
      //TODO: constructor will do the flip, could it do more?
      Data(bb)
    }
  }
  
  protected def parseTime(record: JdbcAdapter2.JdbcRecord, vt: VariableType): Option[Data] = {
    val name = getVariableName(vt) //accounts for rename
    //special handling for Timestamp
    dbTypes.get(name) match {
      case Some(java.sql.Types.TIMESTAMP) =>
        record.getTimestamp(name) map { time =>
//TODO: allow text time (formatted units) constructed from java time (ms since 1970)
//  allow any units as specified in the model
//          vt.getMetadata("units") match {
//            case Some(format) => TimeFormat(format).format(time)
//            case None => TimeFormat.ISO.format(time) //default to ISO yyyy-MM-ddTHH:mm:ss.SSS
//          }
          Data(time)
        }
    }
  }


  /**
   * Lazily create a map of actual column name (or rename) to SQL type.
   * This is used to identify times using TIMESTAMP.
   */
  private lazy val dbTypes: Map[String,Int] = {
    val md = resultSet.getMetaData
    (0 until md.getColumnCount).map(i => (md.getCatalogName(i), i)).toMap
  }
  

  //Handle the Projection and Selection Operation-s.
  //Project all if there is no projection defined.
  //Preserve the order of variables in the model.
  //Support aliases for variables in the model.
/*
 * TODO: how does rename affect this? 
 * consider order: rename after selection
 * these need to be the original col names for rename to work with predicate munging
 * note, this requires that the variable id is the col name
 * //TODO: consider "origName" in metadata like netcdf adapter?
 */
  private var projectedVariableNames = Seq[String]()
  protected def getProjectedVariableNames: Seq[String] = {
    val allScalars = model.getScalars.filter(_.getType != "index")
    val ss =
      if (projectedVariableNames.isEmpty) allScalars
      else allScalars.filter(s => projectedVariableNames.exists(s.hasName(_)))
    ss.map(_.getId)
  }

  protected val selections = ArrayBuffer[String]()

  //Keep map to store Rename operations until they are needed when constructing the sql.
  private val renameMap = mutable.Map[String, String]()

  /**
   * Use this to get the name of a Variable so we can apply rename.
   * This should be the name that appears in the ResultSet metadata,
   * either the original column name or the rename.
   */
  protected def getVariableName(v: VariableType): String = renameMap.get(v.getId) match {
    case Some(newName) => newName
    case None => v.getId
  }

  /**
   * Limit the number of rows returned.
   * This can be defined as an adapter property or from other operations.
   */
  private var limit = getProperty("limit") match {
    case Some(s) => s.toInt //TODO: handle error
    case None    => java.lang.Integer.MAX_VALUE
  }
  
  //Define sorting order.
  private var order = "ASC"

  /**
   * Handle the operations if we can so we can reduce the data volume at the source
   * so the parent adapter doesn't have to do as much.
   * Return true if this adapter is taking responsibility for applying the operation
   * or false if it won't.
   */
  override def handleOperation(operation: Operation): Boolean = operation match {
    case s: Selection  => handleSelection(s)

    case Projection(names) =>
      //TODO: we could have multiple projections (but can we preserve order of operation?)
      //TODO: support compound names
      //These names need to match the db col names which should be the variable IDs (unless we add "origName").
      //Match variables by alias, but keep their ID.
      val scalars = model.getScalars
      val projectedVariables = names.flatMap(s => scalars.find(_.hasName(s)))
      projectedVariableNames = projectedVariables.map(_.getId)
      false //the default Projection Operation will also be applied after derived fields are created.

    case _: FirstFilter =>
      //make sure we are using ascending order
      order = "ASC"; 
      //add a limit property of one so we only get the first record
      limit = 1
      //let the caller know that we handled this operation
      true
      
    case _: LastFilter =>
      //get results in descending order so the first record is the "last" one
      order = "DESC"; 
      //add a limit property of one so we only get the first (now last) record
      limit = 1
      //let the caller know that we handled this operation
      true 

    case LimitFilter(l) =>
      // If limit is already defined, make sure we don't increase it.
      if (l < limit) limit = l
      true //true either way
      
    //Rename operation: apply in projection clause of sql: 'select origName as newName'
    case RenameOperation(origName, newName) =>
      renameMap += (origName -> newName)
      true
      
    //TODO: handle take, takeRight
    //TODO: handle exception, return false (not handled)?

    case _ => false //not an operation that we can handle
  }

  
  def handleSelection(selection: Selection): Boolean = selection match {
    case Selection(name, op, value) => 
      model.getScalars.find(_.hasName(name)) match { //TODO: should we match id?, consider rename
        //TODO: require that vars are projected to be consistent with other adapters? order now matters
        case Some(vt) if (vt.hasName("time")) => handleTimeSelection(name, op, value)
        case Some(vt) =>
          //add a selection to the sql, may need to change operation
          op match {
            case "==" => vt.getType match {
              case "text" => selections append name + "=" + StringUtils.quoteStringValue(value); true
              case _      => selections append name + "=" + value; true
            }
            case "=~" =>
              selections append name + " like '%" + value + "%'"; true
            case "~" => false //almost equal (e.g. nearest sample) not supported by sql
            case _ => vt.getType match {
              case "text" => selections append name + op + StringUtils.quoteStringValue(value); true
              case _      => selections append name + op + value; true
            }
          }
        case None => false //variable not found so no selection to add, but there may be derived variables
        //TODO: warn? logger.warn("JdbcAdapter can't process selection for unknown parameter: " + name)
      }
  }
  
  /**
   * Special handling for a time selection since there are various formatting issues.
   * Value may be native numeric units or an ISO string.
   */
  def handleTimeSelection(vname: String, op: String, value: String): Boolean = {
    //Get the Time variable with the given name
    //TODO: consider rename and projection issues
    
    val tvar = model.getScalars.find(_.hasName(vname)).get //Note, we wouldn't be here if this wasn't a Time variable.
    val tvname = getVariableName(tvar) //potentially renamed variable name
    
    dbTypes(tvname) match {
      case java.sql.Types.TIMESTAMP =>
        // Format the time consistent with java.sql.Timestamp.toString: 
        //   yyyy-mm-dd hh:mm:ss.fffffffff
      // Not for Oracle! webtcad-mms SpacecraftEvents.tsml
        // JDBC doesn't generally like the 'T' in the iso time. (e.g. Derby)
        // Assumes GMT.
//          val time = tvar.getMetadata("units") match {
//          case Some(format) => Time.fromIso(value).format(format)
//          case None => Time.fromIso(value).format("yyyy-MM-dd HH:mm:ss.SSS") //Default that seems to work for most databases, not Oracle
//          //TODO: too late, Time will add units if none are defined, defaulting to the ISO that doesn't generally work
//          //  require tsml to define other units
//        }
//        selections += tvname + op + "'" + time + "'" //sql wants quotes around time value
//        true
        false
      
      case _ =>
        //So, we have a numeric time variable but need to figure out if the selection value is
        //  a numeric time (in native units) or an ISO time that needs to be converted.
        if (StringUtils.isNumeric(value)) {
          this.selections += tvname + op + value
          true
        } else tvar.getMetadata("units") match {
          //Assumes selection value is an ISO 8601 formatted string
          case None => throw new Error("The dataset does not have time units defined for: " + tvname)
          case Some(units) =>
            //convert ISO time selection value to dataset units
            try {
              val t = Time.fromIso(value).convert(TimeScale(units)).getValue
              this.selections += tvname + op + t
              true
            } catch {
              case iae: IllegalArgumentException => throw new Error("The time value is not in a supported ISO format: " + value)
              case e: Exception => throw new Error("Unable to parse time selection: " + value)
            }
        }
    }
  }

//  /**
//   * Override to apply projection. Exclude Variables not listed in the projection.
//   * Only works for Scalars, for now.
//   * If the rename operation needs to be applied, a new temporary Scalar will be created
//   * with a copy of the original's metadata with the 'name' changed.
//   */
//  override def makeScalar(s: ScalarType): Option[Scalar] = {
//    //TODO: deal with composite names for nested vars
////TODO: can we say "false" on the rename op and let latis apply it?
//    getProjectedVariableNames.find(s.hasName(_)) match { //account for aliases
//      case Some(_) => { //projected, see if it needs to be renamed
//        val tmpScalar = renameMap.get(s.getName) match {
//          case Some(newName) => s.getMetadata("alias") match { //keep the old name as an alias to allow later projection. 
//            case Some(a) => s.updatedMetadata("alias" -> (a + "," + s.getName)).updatedMetadata("name" -> newName)
//            case None => s.updatedMetadata("alias" -> s.getName).updatedMetadata("name" -> newName)
//          }
//          case None => s
//        }
//        super.makeScalar(tmpScalar)
//      }
//      case None => None //not projected
//    }
//  }

  //---- Database Stuff -------------------------------------------------------

  /**
   * Execute the SQL query.
   */
  private def executeQuery: ResultSet = {
    val sql = makeQuery
    logger.debug("Executing sql query: " + sql)

    //Apply optional limit to the number of rows
    //TODO: Figure out how to warn the user if the limit is exceeded
    getProperty("limit") match {
      case Some(limit) => statement.setMaxRows(limit.toInt)
      case _ =>
    }

    //Allow specification of number of rows to fetch at a time.
    getProperty("fetchSize") match {
      case Some(fetchSize) => statement.setFetchSize(fetchSize.toInt)
      case _ =>
    }

    statement.executeQuery(sql)
  }

  /**
   * Get the name of the database table from the adapter tsml attributes.
   */
  def getTable: String = getProperty("table") match {
    case Some(s) => s
    case None => throw new Error("JdbcAdapter needs to have a 'table' defined.")
  }

  /**
   * Build the select clause.
   * If no projection operation was provided, include all
   * since the tsml might expose only some database columns.
   * Apply rename operations.
   */
  protected def makeProjectionClause: String = {
    getProjectedVariableNames.map(name => {
      //If renamed, replace 'name' with 'name as "name2"'.
      //Use quotes so we can use reserved words like "min" (needed by Sybase).
      renameMap.get(name) match {
        case Some(name2) => name + " as \"" + name2 + "\""
        case None => name
      }
    }).mkString(",")
  }

  /**
   * Allow tsml to specify a "hint" property to add to the SQL between the "select" 
   * and projection clause. Appropriate for Oracle, at least.
   * For example: 
   *   select /*+INDEX (TMDISCRETE TMDISCRETE_ALL_IDX)*/ * from TMDISCRETE ...
   */
  protected def makeHint: String = getProperty("hint") match {
    case Some(hint) => hint + " " //add white space so it plays nice in makeQuery
    case None => ""
  }
  
  /**
   * Construct the SQL query.
   * Look for "sql" defined in the tsml, otherwise construct it.
   */
  protected def makeQuery: String = getProperty("sql") match {
    case Some(sql) => sql
    case None => {
      //build query
      val sb = new StringBuffer("select ")
      sb append makeHint //will include trailing space if not empty
      sb append makeProjectionClause
      sb append " from " + getTable

      val p = makePredicate
      if (p.nonEmpty) sb append " where " + p
      

      //Sort by domain variable.
      //assume domain is scalar, for now
      //Note 'dataset' should be the original before ops
      model.variable match {
        case f: Function => f.getDomain match {
          case i: Index => //implicit placeholder, use natural order
          case v: Variable => v match {
            //Note, shouldn't matter if we sort on original name
            case _: Scalar => sb append " ORDER BY " + v.getName + " " + order
            case Tuple(vars) => {
              //assume all are scalars, reasonable for a domain variable
              val names = vars.map(_.getName).mkString(", ")
              sb append " ORDER BY " + names + " " + order
            }
          }
        }
        case _ => //no function so no domain variable to sort by
      }
      sb.toString
    }
  }

  /**
   * Build a list of constraints for the "where" clause.
   */
  protected def makePredicate: String = predicate
  private lazy val predicate: String = {
    //Get selection clauses (e.g. from requested operations)
    //Prepend any tsml defined predicate.
    val clauses = getProperty("predicate") match {
      case Some(s) => s +=: selections
      case None => selections
    }

    //insert "AND" between the clauses
    clauses.filter(_.nonEmpty).mkString(" AND ")
  }

  //---------------------------------------------------------------------------

  /**
   * The JDBC ResultSet from the query. This will lazily execute the query
   * when this result is requested.
   */
  private lazy val resultSet: ResultSet = executeQuery
  private lazy val statement: Statement = connection.createStatement()
  //Keep database resources global so we can close them.

  /**
   * Allow subclasses to use the connection. They should not close it.
   */
  protected def getConnection: Connection = connection

  /*
   * Used so we don't end up getting the lazy connection when we are testing if we have one to close.
   */
  private var hasConnection = false

  /*
   * Database connection from JNDI or JDBC properties.
   */
  private lazy val connection: Connection = {
    
    val startsWithJavaRegex = "^(java:.+)".r
    val startsWithJdbcRegex = "^jdbc:(.+)".r
    
    // location is the current standard for both jndi and jdbc connections,
    // but we still support the jndi attribute for historical reasons.
    // See LATIS-30 for more details
    val con = getProperty("location") match {
      
      // if 'location' exists and starts with "java:", use jndi
      case Some(startsWithJavaRegex(location)) => getConnectionViaJndi(location)
      
      // if 'location' exists and starts with 'jdbc:'
      case Some(startsWithJdbcRegex(_)) => getConnectionViaJdbc
      
      // If we get here, we probably have a malformed tsml file. No conforming location attr was found.
      case _ => throw new RuntimeException(
        "Unable to find or parse tsml location attribute: location must exist and start with 'java:' (for JNDI) or 'jdbc:' (for JDBC)"
      )
    }

    hasConnection = true //will still be false if getting connection fails
    con
  }

  private def getConnectionViaJndi(jndiName: String): Connection = {
    val initCtx = new InitialContext()
    var ds: DataSource = null

    try {
      ds = initCtx.lookup(jndiName).asInstanceOf[DataSource]
    } catch {
      case e: NameNotFoundException => throw new Error("JdbcAdapter failed to locate JNDI resource: " + jndiName)
    }

    ds.getConnection()
  }

  private def getConnectionViaJdbc: Connection = {
    val driver = getProperty("driver") match {
      case Some(s) => s
      case None => throw new Error("JdbcAdapter needs to have a JDBC 'driver' defined.")
    }
    val url = getProperty("location") match {
      case Some(s) => s
      case None => throw new Error("JdbcAdapter needs to have a JDBC 'url' defined.")
    }
    val user = getProperty("user") match {
      case Some(s) => s
      case None => throw new Error("JdbcAdapter needs to have a 'user' defined.")
    }
    val passwd = getProperty("password") match {
      case Some(s) => s
      case None => throw new Error("JdbcAdapter needs to have a 'password' defined.")
    }

    //Load the JDBC driver 
    Class.forName(driver)

    //Make database connection
    DriverManager.getConnection(url, user, passwd)
  }

  /**
   * Release the database resources.
   */
  override def close(): Unit = {
    //TODO: http://stackoverflow.com/questions/4507440/must-jdbc-resultsets-and-statements-be-closed-separately-although-the-connection
    if (hasConnection) {
      try { resultSet.close } catch { case e: Exception => }
      try { statement.close } catch { case e: Exception => }
      try { connection.close } catch { case e: Exception => }
    }
  }
}

//=============================================================================  

/**
 * Define some inner classes to provide us with Record semantics for JDBC ResultSets.
 */
object JdbcAdapter2 {

  //TODO: add property for other time zones
  private val gmtCalendar = Calendar.getInstance(TimeZone.getTimeZone("GMT"))

  case class JdbcRecord(resultSet: ResultSet) {
    def getDouble(name: String) = Option(resultSet.getDouble(name))
    def getLong(name: String)   = Option(resultSet.getLong(name))
    def getString(name: String) = Option(resultSet.getString(name))
    def getBytes(name: String)  = Option(resultSet.getBytes(name))
    def getTimestamp(name: String) = 
      Option(resultSet.getTimestamp(name, gmtCalendar).getTime)
  }

  class JdbcEmptyIterator() extends Iterator[JdbcAdapter2.JdbcRecord] {
    private var _hasNext = false
    
    def next(): JdbcRecord = null
    def hasNext(): Boolean = _hasNext
  }

  class JdbcRecordIterator(resultSet: ResultSet) extends Iterator[JdbcAdapter2.JdbcRecord] {
    private var _didNext = false
    private var _hasNext = false

    def next(): JdbcRecord = {
      if (!_didNext) resultSet.next
      _didNext = false
      JdbcRecord(resultSet)
    }

    def hasNext(): Boolean = {
      if (!_didNext) {
        _hasNext = resultSet.next
        _didNext = true
      }
      _hasNext
    }
  }
}
