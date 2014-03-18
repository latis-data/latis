package latis.reader

import latis.dm._
import latis.reader.tsml._
import latis.writer._
import org.junit._
import Assert._
import scala.collection.mutable.ArrayBuffer
import latis.ops._
import java.io.ByteArrayOutputStream
import java.io.FileOutputStream
import java.io.BufferedReader
import java.io.InputStreamReader
import scala.io.Source
import java.io.PrintWriter
import java.sql.DriverManager
import java.sql.Connection
import latis.util.LatisProperties

class TestConcurrency {

  //@Test
  def test_sequential_via_direct_access {
    //See: http://mods-jira.lasp.colorado.edu:8080/browse/WEBTCAD-77
    
    val ops575 = List[Operation](Selection("TMID=575"), Selection("time>=2014-02-19T17:00:00"), Selection("time<2014-02-19T19:00:00"))
    val ops592 = List[Operation](Selection("TMID=592"), Selection("time>=2014-02-19T17:00:00"), Selection("time<2014-02-19T19:00:00"))
      
    Writer(new FileOutputStream("/data/tmp/575-1"), "jsond").write(TsmlReader("datasets/quikscat/AnalogTelemetryItem.tsml").getDataset(ops575))
    Writer(new FileOutputStream("/data/tmp/592-1"), "jsond").write(TsmlReader("datasets/quikscat/AnalogTelemetryItem.tsml").getDataset(ops592))
    Writer(new FileOutputStream("/data/tmp/575-2"), "jsond").write(TsmlReader("datasets/quikscat/AnalogTelemetryItem.tsml").getDataset(ops575))
    Writer(new FileOutputStream("/data/tmp/592-2"), "jsond").write(TsmlReader("datasets/quikscat/AnalogTelemetryItem.tsml").getDataset(ops592))
    Writer(new FileOutputStream("/data/tmp/575-3"), "jsond").write(TsmlReader("datasets/quikscat/AnalogTelemetryItem.tsml").getDataset(ops575))
    Writer(new FileOutputStream("/data/tmp/592-3"), "jsond").write(TsmlReader("datasets/quikscat/AnalogTelemetryItem.tsml").getDataset(ops592))
        
    //Writer("json").write(ds)
    //AsciiWriter.write(ds) quikscat/AnalogTelemetryItem.jsond?&TMID=575&time%3E=2014-02-19T17:00:00&time%3C2014-02-19T19:00:00
  }

  
  def make_runnable_server_request(url: String, file: String): Runnable = new Runnable {
    def run {    
      val fos1 = new PrintWriter(file)
      val lines = Source.fromURL(url).getLines.toList
      lines.map(fos1.println(_))
      fos1.close
    }
  }
  
  //@Test
  def test_concurrent_via_server {
    val url575 = "http://localhost:8080/web-tcad/dap/quikscat/AnalogTelemetryItem.jsond?&TMID=575&time>=2014-02-19T17:00:00&time<2014-02-19T19:00:00"
    val url592 = "http://localhost:8080/web-tcad/dap/quikscat/AnalogTelemetryItem.jsond?&TMID=592&time>=2014-02-19T17:00:00&time<2014-02-19T19:00:00"

    new Thread(make_runnable_server_request(url575, "/data/tmp/575-1")).start()
    new Thread(make_runnable_server_request(url592, "/data/tmp/592-1")).start()
    new Thread(make_runnable_server_request(url575, "/data/tmp/575-2")).start()
    new Thread(make_runnable_server_request(url592, "/data/tmp/592-2")).start()
    new Thread(make_runnable_server_request(url575, "/data/tmp/575-3")).start()
    new Thread(make_runnable_server_request(url592, "/data/tmp/592-3")).start()
      
    //TODO: need to have unit test wait till all threads are complete?
    Thread.sleep(5000)
  }
   
  //=======================================================================================================

  def make_runnable_direct_request_sybase(ops: Seq[Operation], file: String): Runnable = new Runnable {
    def run {
      val writer = Writer(new FileOutputStream(file), "csv")
      writer.write(TsmlReader("/home/lindholm/git/web-tcad/src/main/resources/datasets/quikscat/tmanalog.tsml").getDataset(ops))
    }
  }
  
  @Test //Both results corrupted
  def test_concurrent_direct_sybase {
    //trigger loading of properties and log config so that doesn't collide, only avoids log config errors
    LatisProperties.getOrElse("foo", "bar")

    val ops575 = List[Operation](Selection("TMID=575"), Selection("time>=2014-02-19T17:00:00"), Selection("time<2014-02-19T19:00:00"))
    val ops592 = List[Operation](Selection("TMID=592"), Selection("time>=2014-02-19T17:00:00"), Selection("time<2014-02-19T19:00:00"))
    
    new Thread(make_runnable_direct_request_sybase(ops575, "/data/tmp/test1")).start()
    //Thread.sleep(5000) //ok after 5 sec wait
    new Thread(make_runnable_direct_request_sybase(ops575, "/data/tmp/test2")).start()
      
    //TODO: need to have unit test wait till all threads are complete?
    Thread.sleep(5000)
  }
  
  //=======================================================================================================
  
  def make_runnable_jdbc_request_with_single_connection(con: Connection, sql: String, file: String): Runnable = new Runnable {
    def run {
      val pw = new PrintWriter(file)
      val rs = con.createStatement.executeQuery(sql)
      while (rs.next) {
        pw.println(rs.getString(2) + "," + rs.getString(3))
      }
      pw.close
    }
  }
  
  //@Test //No problems
  def direct_jdbc_with_same_connection {
    val jdbc = "jdbc:sybase:Tds:lasp-db2:4100/QuikSCAT"
      
    val driver = "com.sybase.jdbc3.jdbc.SybDriver"
    Class.forName(driver)

    //Make database connection
    val user="qs" 
    val passwd="quikscatdb"
    val con = DriverManager.getConnection(jdbc, user, passwd)
    val sql = "select * from TManalog where TMID=575 AND DT>='2014-02-19 17:00:00.000' AND DT<'2014-02-19 19:00:00.000' ORDER BY DT ASC"
      
    new Thread(make_runnable_jdbc_request_with_single_connection(con, sql, "/data/tmp/test1")).start()
    new Thread(make_runnable_jdbc_request_with_single_connection(con, sql, "/data/tmp/test2")).start()
    Thread.sleep(5000)
  }  
  
  def make_runnable_jdbc_request_with_new_connection(sql: String, file: String): Runnable = new Runnable {
    def run {    
      val jdbc = "jdbc:sybase:Tds:lasp-db2:4100/QuikSCAT"
      
      val driver = "com.sybase.jdbc3.jdbc.SybDriver"
      Class.forName(driver)
      val user="qs" 
      val passwd="quikscatdb"
      val con = DriverManager.getConnection(jdbc, user, passwd)
      val rs = con.createStatement.executeQuery(sql)
      
      val pw = new PrintWriter(file)
      while (rs.next) {
        pw.println(rs.getString(2) + "," + rs.getString(3))
      }
      pw.close
    }
  }
  
  //@Test  //no problems
  def direct_jdbc_with_new_connection {

    val sql = "select * from TManalog where TMID=575 AND DT>='2014-02-19 17:00:00.000' AND DT<'2014-02-19 19:00:00.000' ORDER BY DT ASC"
      
    new Thread(make_runnable_jdbc_request_with_new_connection(sql, "/data/tmp/test1")).start()
    new Thread(make_runnable_jdbc_request_with_new_connection(sql, "/data/tmp/test2")).start()
    new Thread(make_runnable_jdbc_request_with_new_connection(sql, "/data/tmp/test3")).start()
    Thread.sleep(5000)
  }
}