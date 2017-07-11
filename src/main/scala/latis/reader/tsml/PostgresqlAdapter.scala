package lasp.latis.reader.tsml

import latis.reader.tsml.JdbcAdapter
import latis.reader.tsml.ml.Tsml

/**
 * Extend JdbcAdapter to change how row limits are applied for
 * Postgresql database queries. The Postgresql JDBC driver does
 * not appear to use the java.sql.Statement's setMaxRows which 
 * we use in the JdcbAdapter. This adapter will add "limit n" 
 * to the sql (which isn't supported by Oracle).
 */
class PostgresqlAdapter(tsml: Tsml) extends JdbcAdapter(tsml) {

  /**
   * Override to apply the 'limit' property if it is set.
   * Limit, First, and Last Filters use this property.
   */
  override def makeQuery: String = {
    //Delegate to the superclass to construct the main query.
    val sql = super.makeQuery
    
    //Append the limit to the query if the property is set.
    getProperty("limit") match {
      case Some(limit) => sql + " limit " + limit.toInt
      case None => sql
    }
  }

}
