package lasp.latis.reader.tsml

import latis.reader.tsml.JdbcAdapter
import latis.reader.tsml.ml.Tsml

/**
 * Extend JdbcAdapter to change how row limits are applied for
 * Postgresql database queries.
 */
class PostgresqlAdapter(tsml: Tsml) extends JdbcAdapter(tsml) {

  /**
   * Override to apply the 'limit' property if it is set.
   * Limit, First, and Last Filters use this property.
   */
  override def makeQuery = {
    val sql = super.makeQuery
    
    getProperty("limit") match {
      case Some(limit) => sql + " limit " + limit.toInt
      case None => sql
    }
  }

}
